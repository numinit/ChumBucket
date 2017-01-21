﻿using ChumBucket.Util;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;
using WebRole.Util;

namespace ChumBucket.Util {
    public class DLJobAdapter {
        private static string DATA_LAKE_BUCKET = "dl";
        private static string JOB_RESULT_BUCKET = "result";

        private IDirectUriFactory _factory;
        private DLClient _client;
        private DLStorageAdapter _resultStorage;
        private DataLakeAnalyticsJobManagementClient _jobClient;
        private IFileSystemOperations _fs;

        public DLJobAdapter(IDirectUriFactory factory, DLClient client, DLStorageAdapter resultStorage) {
            this._factory = factory;
            this._client = client;
            this._resultStorage = resultStorage;
            this._jobClient = client.JobClient;
            this._fs = this._client.FsClient.FileSystem;
        }

        public JobEntityUri SubmitJob(string jobName, string code,
                                      int priority = 1, int parallelism = 1) {
            // Create a job id
            var jobId = Guid.NewGuid();
            var jobKey = jobId.ToString();

            // Create an empty output file named after the job ID.
            // This will let us use the job ID to query the Data Lake storage adapter later.
            var jobStorageUri = this.KeyToAdlUri(jobKey);
            var storageFile = new StorageFile(new MemoryStream(), jobStorageUri, jobName, "text/csv");
            this._resultStorage.Store(storageFile, JOB_RESULT_BUCKET);

            // Now, submit the job
            var properties = new USqlJobProperties(this.BuildScript(this._factory, jobId, jobStorageUri, code));
            var parameters = new JobInformation(jobName, JobType.USql, properties,
                priority: priority, degreeOfParallelism: parallelism, jobId: jobId);
            var jobInfo = this._jobClient.Job.Create(this._client.AccountName, jobId, parameters);

            // Return a chumbucket+job://dl/{jobKey} URI for this job.
            return new JobEntityUri(bucket: DATA_LAKE_BUCKET, key: jobKey);
        }

        public JobInformation GetJobInfo(JobEntityUri uri) {
            // Get the job GUID from the URI
            if (uri.Bucket != DATA_LAKE_BUCKET) {
                throw new ArgumentException("invalid bucket");
            }

            var guid = new Guid(uri.Key);
            var ret = this._jobClient.Job.Get(this._client.AccountName, guid);
            if (ret == null) {
                throw new KeyNotFoundException("job does not exist");
            }
            return ret;
        }

        public StorageFile GetJobResult(JobEntityUri uri) {
            if (uri.Bucket != DATA_LAKE_BUCKET) {
                throw new ArgumentException("invalid bucket");
            }

            var adlUri = this.KeyToAdlUri(uri.Key);
            return this._resultStorage.Retrieve(adlUri);
        }

        private EntityUri KeyToAdlUri(string key) {
            return new DLEntityUri(bucket: JOB_RESULT_BUCKET, key: string.Format("{0}.csv", key));
        }

        static Regex INPUT_REGEX = new Regex(@"@in\s*\[\s*""\s*([^""]*)""\s*\]", RegexOptions.Compiled);

        private string BuildScript(IDirectUriFactory factory, Guid jobId, EntityUri outputUri, string code) {
            string script = string.Format(@"
//
// chumbucket job {0}
// Output: {1}
//
USE master;
DECLARE @out string = @""{1}"";
DECLARE @in = new SQL.MAP<string, string> {", jobId.ToString(), outputUri.ToString()).TrimStart();

            // Scan the code for instances of the input map, and add references
            var matches = INPUT_REGEX.Matches(code);
            foreach (Match match in matches) {
                // Replace internal slashes
                var key = match.Captures[1].Value.Replace(@"\", @"/");
                var uri = this.Resolve(key);
                script += "\r\n";
                script += string.Format(@"    {@""{0}"", @""{1}""},", this.Escape(key), this.Escape(uri.ToString()));
            }
            script += "\r\n";
            script += "};";
            script += "\r\n\r\n";
            script += code;
            script += "\r\n\r\n";
            script += "OUTPUT @result TO @out USING Outputters.Csv();";
            return script;
        }

        private string Escape(string str) {
            return str.Replace("\"", "\"\"");
        }

        private Uri Resolve(string path) {
            if (path.Length == 0) {
                throw new ArgumentException("path may not be empty");
            }

            string[] components = path.Split('/');
            string bucket = null, key = null;
            if (components.Length == 1) {
                // This references a bucket
                bucket = components[0];
            } else if (components.Length == 2) {
                // This references a file inside a bucket
                bucket = components[0];
                key = components[1];
            } else {
                throw new ArgumentException(string.Format("path {0} neither refers to a bucket nor a file", key));
            }

            if (bucket.Length == 0) {
                throw new ArgumentException("bucket name may not be empty");
            } else if (key != null && key.Length == 0) {
                throw new ArgumentException("filename may not be empty");
            }

            return this._factory.BuildDirectUri(bucket, key);
        }
    }
}