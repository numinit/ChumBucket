using ChumBucket.Util;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using Microsoft.Azure.Management.DataLake.Store;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;

namespace WebRole.Util {
    public class DLJobAdapter {
        private DLClient _client;
        private DLStorageAdapter _resultStorage;
        private DataLakeAnalyticsJobManagementClient _jobClient;
        private IFileSystemOperations _fs;

        public DLJobAdapter(DLClient client, DLStorageAdapter resultStorage) {
            this._client = client;
            this._resultStorage = resultStorage;
            this._jobClient = client.JobClient;
            this._fs = this._client.FsClient.FileSystem;
        }

        public Uri SubmitJob(string jobName, Uri inputUri, string code,
                              int priority = 1, int parallelism = 1) {
            // Create a job id
            var jobId = Guid.NewGuid();

            // Create an empty output file named after the job ID.
            // This will let us use the job ID to query the Data Lake storage adapter later.
            var storageFile = new StorageFile(new MemoryStream(), jobName, "text/csv");
            var outputUri = this._resultStorage.Store(storageFile, jobId);

            // Now, submit the job
            var properties = new USqlJobProperties(this.BuildScript(jobId, inputUri, outputUri, code));
            var parameters = new JobInformation(jobName, JobType.USql, properties,
                priority: priority, degreeOfParallelism: parallelism, jobId: jobId);
            var jobInfo = this._jobClient.Job.Create(this._client.AccountName, jobId, parameters);

            // Return a chumbucket://job/<uuid> URI.
            return this.BuildUri(jobId);
        }

        public JobInformation GetJobInfo(Guid jobId) {
            return this._jobClient.Job.Get(this._client.AccountName, jobId);
        }

        private Uri BuildUri(Guid guid) {
            var builder = new UriBuilder();
            builder.Scheme = "chumbucket";
            builder.Host = "job";
            builder.Path = string.Format("/{0}", guid.ToString());
            return builder.Uri;
        }

        private string BuildScript(Guid jobId, Uri inputUri, Uri outputUri, string code) {
            string preamble = @"
//
// chumbucket job {0}
// Input:  {1}
// Output: {2}
//
DECLARE @in string = @""{1}"";
DECLARE @out string = @""{2}"";

{3}

OUTPUT @result TO @out USING Outputters.Csv();
";
            // yolo
            return string.Format(preamble, jobId.ToString(), inputUri.ToString(), outputUri.ToString(), code);
        }
    }
}