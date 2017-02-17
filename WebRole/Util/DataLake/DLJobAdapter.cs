using ChumBucket.Util.Storage;
using ChumBucket.Util.Uris;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace ChumBucket.Util.DataLake {
    /**
     * An adapter around the Data Lake Job Management Client
     * for submitting jobs in ChumBucket.
     */
    public class DLJobAdapter {
        private static readonly string DATA_LAKE_AUTHORITY = "dl";
        private static readonly string JOB_RESULT_BUCKET = "result";

        private readonly IDirectUriFactory _blobStorageFactory, _dlStorageFactory;
        private readonly DLClient _client;
        private readonly DLStorageAdapter _resultStorage;
        private readonly DataLakeAnalyticsJobManagementClient _jobClient;

        /**
         * Initializes this DLJobAdapter.
         * <param name="blobStorageFactory">The blob storage URI factory</param>
         * <param name="client">The Data Lake client</param>
         * <param name="resultStorage">The result storage adapter</param>
         */
        public DLJobAdapter(IDirectUriFactory blobStorageFactory, DLClient client, DLStorageAdapter resultStorage) {
            this._blobStorageFactory = blobStorageFactory;
            this._dlStorageFactory = new DLStorageUriFactory(resultStorage);
            this._client = client;
            this._resultStorage = resultStorage;
            this._jobClient = client.JobClient;
        }

        /**
         * Submits a job
         * <param name="jobName">The name of the job</param>
         * <param name="code">The code</param>
         * <param name="priority">The job priority</param>
         * <param name="parallelism">The parallelism</param>
         */
        public JobEntityUri SubmitJob(string jobName, string code,
                                      int priority = 1, int parallelism = 1) {
            // Create a job id
            var jobId = Guid.NewGuid();
            var jobKey = jobId.ToString();

            // Create an empty output file named after the job ID.
            // This will let us use the job ID to query the Data Lake storage adapter later.
            var jobStorageUri = KeyToAdlUri(jobKey);
            var storageFile = new StorageFile(new MemoryStream(), jobStorageUri, jobKey, "text/csv");
            this._resultStorage.Store(storageFile, JOB_RESULT_BUCKET);

            // Now, submit the job
            var properties = new USqlJobProperties(this.BuildScript(jobId, jobStorageUri, code));
            var parameters = new JobInformation(jobName, JobType.USql, properties,
                priority: priority, degreeOfParallelism: parallelism, jobId: jobId);
            var jobInfo = this._jobClient.Job.Create(this._client.AccountName, jobId, parameters);

            // Return a chumbucket+job://dl/{jobKey} URI for this job.
            return GuidToJobUri(jobId);
        }

        /**
         * Returns a DLJobStatus instance from the specified JobEntityUri.
         * <param name="uri">The URI</param>
         * <returns>A DLJobStatus instance</returns>
         */
        public DLJobStatus GetJobStatus(JobEntityUri uri) {
            // Get the job GUID from the URI
            var guid = ValidateJobUri(uri);
            var info = this._jobClient.Job.Get(this._client.AccountName, guid);
            if (info == null) {
                throw new KeyNotFoundException("job does not exist");
            }

            var name = info.Name;
            DateTime startTime = info.SubmitTime.Value.DateTime;
            DateTime? endTime = null;
            var state = info.State.Value;
            var statusString = "UNKNOWN";
            if (state == JobState.Ended) {
                statusString = info.Result.ToString().ToUpper();
                endTime = info.EndTime.Value.DateTime;
            } else {
                statusString = state.ToString().ToUpper();
            }

            if (info.Result.HasValue && info.Result.Value == JobResult.Failed) {
                // The job failed
                return new DLJobStatus(uri: uri, name: name, status: statusString,
                                       startTime: startTime, endTime: endTime,
                                       error: BuildErrorMessage(info));
            } else if (info.Result.HasValue && info.Result.Value == JobResult.Succeeded) {
                // The job succeeded; collect statistics
                var stats = this._jobClient.Job.GetStatistics(this._client.AccountName, guid);
                long bytes = 0;
                double seconds = 0;
                foreach (var stage in stats.Stages) {
                    if (stage.DataRead.HasValue && stage.TotalSucceededTime.HasValue) {
                        bytes += stage.DataRead.Value;
                        seconds += stage.TotalSucceededTime.Value.TotalSeconds;
                    }
                }
                return new DLJobStatus(uri: uri, name: name, status: statusString,
                                       startTime: startTime, endTime: endTime,
                                       bytes: bytes, throughput: bytes / seconds);
            } else {
                // The job is in progress
                return new DLJobStatus(uri: uri, name: name, status: statusString,
                                       startTime: startTime, endTime: endTime);
            }
        }

        /**
         * Returns a StorageFile from the specified JobEntityUri.
         * <param name="uri">The URI</param>
         * <returns>The corresponding StorageFile</returns>
         */
        public StorageFile GetJobResult(JobEntityUri uri) {
            ValidateJobUri(uri);

            var adlUri = KeyToAdlUri(uri.Key);
            return this._resultStorage.Retrieve(adlUri);
        }

        private static Guid ValidateJobUri(JobEntityUri uri) {
            if (uri.Bucket != DATA_LAKE_AUTHORITY) {
                throw new ArgumentException("invalid bucket name");
            } else {
                try {
                    return Guid.Parse(uri.Key);
                } catch (Exception e) when (e is FormatException || e is ArgumentNullException) {
                    throw new ArgumentException(e.Message);
                }
            }
        }

        /**
         * Converts a GUID to a JobEntityUri.
         * <param name="guid">The GUID</param>
         * <returns>A JobEntityUri</returns>
         */
        private static JobEntityUri GuidToJobUri(Guid guid) {
            return new JobEntityUri(bucket: DATA_LAKE_AUTHORITY, key: guid.ToString());
        }

        /**
         * Converts a key to an Azure Data Lake URI.
         * <param name="key">The key</param>
         * <returns>A DLEntityUri</returns>
         */
        private static EntityUri KeyToAdlUri(string key) {
            return new DLEntityUri(bucket: JOB_RESULT_BUCKET, key: $"{key}.csv");
        }

        private static readonly Regex INPUT_REGEX = new Regex(@"@in\s*\[\s*""\s*(?<key>[^""]*)""\s*\]", RegexOptions.Compiled);

        /**
         * Builds a U-SQL script with the ChumBucket preamble and postamble.
         * <param name="jobId">The job ID</param>
         * <param name="outputUri">The output URI</param>
         * <param name="code">The code</param>
         * <returns>The script</returns>
         */
        private string BuildScript(Guid jobId, EntityUri outputUri, string code) {
            // Resolve the output URI to Data Lake Store
            var resolvedOutputUri = this._dlStorageFactory.BuildDirectUri(outputUri.Bucket, outputUri.Key);

            // Piece together the script
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat(@"//
// chumbucket job {0}
// Output: {1}
//
USE master;
DECLARE @out string = @""{1}"";
DECLARE @in SQL.MAP<string, string> = new SQL.MAP<string, string> {{", jobId.ToString(), resolvedOutputUri.ToString());

            // Scan the code for instances of the input map, and add references
            var matches = INPUT_REGEX.Matches(code);
            foreach (Match match in matches) {
                // Replace internal slashes
                var key = match.Groups["key"].Value.Replace(@"\", @"/");
                var uri = this.Resolve(key);
                sb.AppendFormat(@"    {{@""{0}"", @""{1}""}},", this.Escape(key), this.Escape(uri.ToString()));
            }
            sb.AppendLine();
            sb.AppendLine(@"};");
            sb.AppendLine();
            sb.AppendLine("// YOUR CODE STARTS HERE");
            sb.Append(code.Trim());
            sb.AppendLine();
            sb.AppendLine("// YOUR CODE ENDS HERE");
            sb.AppendLine();
            sb.Append(@"OUTPUT @result TO @out USING Outputters.Csv();");
            return sb.ToString();
        }

        /**
         * Escapes quotes for insertion into C# literal strings.
         * <param name="str">The string</param>
         * <returns>The escaped string</returns>
         */
        private string Escape(string str) {
            return str.Replace("\"", "\"\"");
        }

        /**
         * Resolves the specified path, returning a direct URI to the resource.
         * <param name="path">The path</param>
         * <returns>A direct URI</returns>
         */
        private Uri Resolve(string path) {
            if (path.Length == 0) {
                throw new ArgumentException("path may not be empty");
            }

            var components = path.Split('/');
            string bucket = null, key = null;
            if (components.Length == 1) {
                // This references a bucket; wildcard it
                bucket = components[0];
                key = "{*}.csv";
            } else if (components.Length == 2) {
                // This references a file inside a bucket
                bucket = components[0];
                key = components[1];
            } else {
                throw new ArgumentException($"path {key} neither refers to a bucket nor a file");
            }

            if (bucket.Length == 0) {
                throw new ArgumentException("bucket name may not be empty");
            } else if (key != null && key.Length == 0) {
                throw new ArgumentException("filename may not be empty");
            }

            return this._blobStorageFactory.BuildDirectUri(bucket, key);
        }

        /**
         * Builds a very detailed error message using a JobInformation instance.
         * Assumes that the JobInformation has an error message.
         * <param name="info">The JobInformation instance</param>
         * <returns>A very detailed multiline error message</returns>
         */
        private static string BuildErrorMessage(JobInformation info) {
            var builder = new StringBuilder();
            var i = 1;
            foreach (var error in info.ErrorMessage) {
                AppendError(i, error, builder);
                if (error.InnerError != null) {
                    // This is annoying. The InnerError isn't provided as a JobErrorDetails,
                    // so make one ourselves.
                    var innerError = error.InnerError;
                    var innerErrorDetails = new JobErrorDetails(description: innerError.Description, details: innerError.Details,
                                                                errorId: innerError.ErrorId, message: innerError.Message,
                                                                resolution: innerError.Resolution);
                    var innerErrorBuilder = new StringBuilder();

                    // Now, perform the append.
                    AppendError(i, innerErrorDetails, innerErrorBuilder);

                    // Then, indent each line from the inner error.
                    using (var reader = new StringReader(innerErrorBuilder.ToString())) {
                        string line = string.Empty;
                        while (line != null) {
                            line = reader.ReadLine();
                            if (line != null) {
                                builder.AppendFormat("    {0}", line);
                            }
                        }
                    }
                }

                i += 1;
            }

            return builder.ToString();
        }

        /**
         * Appends error number `idx` to `builder` using the specified `error`.
         * <param name="idx">The index</param>
         * <param name="error">The error details</param>
         * <param name="builder">The StringBuilder</param>
         */
        private static void AppendError(int idx, JobErrorDetails error, StringBuilder builder) {
            builder.AppendFormat(@"[{0}] {1}: {2}

=== DESCRIPTION ===
{3}

=== DETAILS ===
{4}", idx, error.ErrorId, error.Message, error.Description, error.Details);

            var resolution = error.Resolution;
            if (!string.IsNullOrEmpty(resolution)) {
                builder.AppendFormat(@"

=== RESOLUTION ===
{0}", resolution);
            }
        }
    }
}
 
 
 