using ChumBucket.Util;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Web;
using WebRole.Util;

namespace WebRole {
    public sealed class AzureConfig {
        public static BlobStorageAdapter BLOB_STORAGE;
        public static DLClient DL;
        public static DLStorageAdapter DL_UPLOAD;
        public static DLStorageAdapter DL_RESULT;
        public static DLJobAdapter DL_JOB;

        public static void CreateClients() {
            // Development
            var blobStorageConnectionString = "UseDevelopmentStorage=true";

            // Deployment
            // var blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=[STORAGE ACCOUNT NAME];AccountKey=[STORAGE ACCOUNT KEY]";

            // We can't emulate Data Lake locally, so have to use a live instance.
            var dlAccountName = "[DATA LAKE STORAGE/ANALYTICS ACCOUNT NAME - MUST BE IDENTICAL]";
            var secret = "[ACTIVE DIRECTORY APPLICATION SECRET]";
            var subId = "[CLOUD SERVICES SUBSCRIPTION UUID]";
            var clientId = "[ACTIVE DIRECTORY APPLICATION UUID]";
            var domain = "[ACTIVE DIRECTORY PRIMARY DOMAIN]";

            // Simple configuration. Creates a Blob storage adapter storing blobs in
            // a "files" directory, and two Data Lake storage adapters rooted at
            // "upload" and "result" for uploads and results.
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            AzureConfig.BLOB_STORAGE = new BlobStorageAdapter(blobStorageConnectionString, "files");
            AzureConfig.DL = new DLClient(dlAccountName, secret, subId, clientId, domain);
            AzureConfig.DL_UPLOAD = new DLStorageAdapter(AzureConfig.DL, "upload");
            AzureConfig.DL_RESULT = new DLStorageAdapter(AzureConfig.DL, "result");
            AzureConfig.DL_JOB = new DLJobAdapter(AzureConfig.DL, AzureConfig.DL_RESULT);
        }
    }
}
