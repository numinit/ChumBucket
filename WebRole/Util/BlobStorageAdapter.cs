using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Web;
using ChumBucket.Util;

namespace ChumBucket.Util {
    public class BlobStorageAdapter : IStorageAdapter {
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;

        public BlobStorageAdapter(string connectionString, string containerName) {
            this._storageAccount = CloudStorageAccount.Parse(connectionString);
            this._blobClient = this._storageAccount.CreateCloudBlobClient();
            this._blobContainer = this._blobClient.GetContainerReference(containerName);
            this._blobContainer.CreateIfNotExists();
        }

        public string GetAccountName() {
            return this._blobClient.Credentials.AccountName;
        }

        public string GetContainerName() {
            return this._blobContainer.Name;
        }

        public void Store(StorageFile file, string bucket) {
            var directory = this._blobContainer.GetDirectoryReference(bucket);
            var blob = directory.GetBlockBlobReference(file.Name);
            using (file.InputStream) {
                blob.Metadata["name"] = file.Name;
                blob.Properties.ContentType = file.ContentType;
                blob.UploadFromStream(file.InputStream);
            }
        }

        public StorageFile Retrieve(EntityUri uri) {
            try {
                // Get a reference to the directory and the blob
                var directory = this._blobContainer.GetDirectoryReference(uri.Bucket);
                var key = uri.Key;
                var blob = directory.GetBlockBlobReference(key);

                if (!blob.Exists()) {
                    throw new KeyNotFoundException("key does not exist");
                } else {
                    var stream = blob.OpenRead();
                    var name = blob.Metadata["name"];
                    if (name == null) {
                        // No other name stored
                        name = key;
                    }
                
                    return new StorageFile(stream, uri, name, blob.Properties.ContentType);
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException) {
                throw new ArgumentException(e.Message);
            }
        }

        public ICollection<EntityUri> ListBuckets() {
            var list = new List<EntityUri>();
            var blobs = this._blobContainer.ListBlobs();
            var buckets = blobs.Where(b => b as CloudBlobDirectory != null);
            foreach (var bucket in buckets) {
                //bucket.Uri
            }
            return list;
        }

        public ICollection<EntityUri> ListFiles(string bucket) {
            var list = new List<EntityUri>();
            var directory = this._blobContainer.GetDirectoryReference(bucket);
            var blobs = directory.ListBlobs().Where(b => b as CloudBlockBlob != null);
            foreach (var blob in blobs) {

            }
            return list;
        }
    }
}