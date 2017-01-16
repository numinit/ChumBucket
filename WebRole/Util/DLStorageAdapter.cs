using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebRole.Util
{
    public class DLStorageAdapter : StorageAdapter
    {
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;

        public DLStorageAdapter(string connectionString, string containerName) {
            this._storageAccount = CloudStorageAccount.Parse(connectionString);
            this._blobClient = this._storageAccount.CreateCloudBlobClient();
            this._blobContainer = this._blobClient.GetContainerReference(containerName);
            this._blobContainer.CreateIfNotExists();
        }

        public string Store(StorageFile file) {
            var guid = Guid.NewGuid();
            var key = guid.ToString();
            var blob = this._blobContainer.GetBlockBlobReference(key);
            using (file.InputStream) {
                blob.Metadata["name"] = file.Name;
                blob.Properties.ContentType = file.ContentType;
                blob.UploadFromStream(file.InputStream);
            }
            return key;
        }

        public StorageFile Retrieve(string key) {
            try {
                var guid = Guid.Parse(key);
                var blob = this._blobContainer.GetBlockBlobReference(key);
                if (!blob.Exists()) {
                    throw new KeyNotFoundException(string.Format("blob {0} does not exist", guid.ToString()));
                } else {
                    var stream = blob.OpenRead();
                    var name = blob.Metadata["name"];
                    if (name == null) {
                        // No other name stored
                        name = guid.ToString();
                    }

                    var contentType = blob.Properties.ContentType;
                    if (contentType == null) {
                        // Reasonable default if there's no type stored
                        contentType = "application/octet-stream";
                    }
                    return new StorageFile(stream, name, contentType);
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException) {
                throw new ArgumentException(e.Message);
            }
        }
    }
}