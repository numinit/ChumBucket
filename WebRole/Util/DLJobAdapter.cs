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
    public class DLJobAdapter : UriKeyable {
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

        public JobInformation GetJobInfo(Uri uri) {
            this.AssertAccepts(uri);

            // Get the job GUID from the URI
            var guid = new Guid(Path.GetFileNameWithoutExtension(uri.AbsolutePath));
            var ret = this._jobClient.Job.Get(this._client.AccountName, guid);
            if (ret == null) {
                throw new KeyNotFoundException(string.Format("job {0} does not exist", guid.ToString()));
            }
            return ret;
        }

        public StorageFile GetJobResult(Uri uri) {
            this.AssertAccepts(uri);

            var guid = new Guid(uri.AbsolutePath.Substring(1));
            var storageUri = this._resultStorage.BuildUri(guid);
            return this._resultStorage.Retrieve(storageUri);
        }

        public override bool WillAccept(Uri uri) {
            return uri.Scheme.Equals("chumbucket") &&
                   uri.Host.Equals("job");
        }

        public override Uri BuildUri(Guid guid) {
            // chumbucket://job/{guid}
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