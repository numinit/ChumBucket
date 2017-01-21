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
using System.Security;
using System.Text.RegularExpressions;
using WebRole.Util;

namespace ChumBucket.Util {
    public class BlobStorageAdapter : IStorageAdapter {
        private IDirectUriFactory _factory;
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;

        public BlobStorageAdapter(string connectionString, string containerName) {
            this._factory = new BlobStorageUriFactory(this);
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

        public BlobStorageEntityUri FinalizeUploadedBlob(Uri blobUri, string name, string contentType) {
            var entityUri = this.GetBlobStorageHandle(blobUri);
            var bucket = this._blobContainer.GetDirectoryReference(entityUri.Bucket);
            var blob = bucket.GetBlockBlobReference(entityUri.Key);
            blob.Metadata["name"] = name;
            blob.Metadata["guid"] = Guid.NewGuid().ToString();
            if (contentType != null) {
                blob.Properties.ContentType = contentType;
            } else {
                blob.Properties.ContentType = "application/octet-stream";
            }
            var metadataTask = blob.SetMetadataAsync();
            var propertiesTask = blob.SetPropertiesAsync();
            metadataTask.Wait();
            propertiesTask.Wait();

            return entityUri;
        }

        public Uri GetRootUri() {
            return this._factory.BuildDirectHttpsUri("");
        }

        /**
         * Returns a direct SAS (shared access signature) for a given URI.
         * If there is an issue with the given blob URI, throws a SecurityException.
         * <param name="blobUri">The blob URI</param>
         * <param name="verb">The HTTP verb</param>
         * <param name="expirationMins">The expiration time, in minutes</param>
         * <returns>A SAS URI</returns>
         */
        public Uri GetSasForBlob(Uri blobUri, string verb, int expirationMins = 15) {
            var entityUri = this.GetBlobStorageHandle(blobUri);
            var bucket = this._blobContainer.GetDirectoryReference(entityUri.Bucket);
            var blob = bucket.GetBlockBlobReference(entityUri.Key);
            var permission = SharedAccessBlobPermissions.Write;

            if (verb == "DELETE") {
                permission = SharedAccessBlobPermissions.Delete;
            }

            // Expire their token after 15 minutes
            var sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy() {
                Permissions = permission,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(expirationMins),
            });

            var rewrittenUri = this._factory.BuildDirectHttpsUri(entityUri.Bucket, entityUri.Key);
            return new Uri(string.Format("{0}{1}", rewrittenUri, sas));
        }

        private static Regex BLOB_CORE_REGEX = new Regex(@"^(?<account>[^\.]+)\.blob\.core\.windows\.net$", RegexOptions.Compiled);

        /**
         * Gets a blob storage entity URI from a direct URI.
         * If there's a mismatch between the direct URI and our configuration,
         * throws a SecurityException.
         * <param name="blobUri">The blob URI</param>
         * <returns>The corresponding BlobStorageEntityUri</returns>
         */
        public BlobStorageEntityUri GetBlobStorageHandle(Uri blobUri) {
            // Ensure that we're signing something reasonable.
            // The URI looks something like: https://{account}.blob.core.windows.net/{container}/{bucket}/{key}
            if (blobUri.Scheme != "https") {
                throw new SecurityException("invalid scheme");
            }

            var match = BLOB_CORE_REGEX.Match(blobUri.Authority);
            if (!match.Success) {
                throw new SecurityException("invalid authority");
            }

            var accountName = match.Groups["account"].Value;
            if (accountName != this._storageAccount.Credentials.AccountName) {
                throw new SecurityException("invalid account name");
            }

            // Remove the leading slash
            var path = blobUri.AbsolutePath.Substring(1);
            var components = path.Split('/');
            if (components.Length != 3) {
                throw new SecurityException("invalid number of path components");
            } else if (components[0] != this._blobContainer.Name) {
                throw new SecurityException("invalid container name");
            }

            // Normalize the bucket name
            string bucketName = null;
            try {
                bucketName = this.NormalizeBucketName(components[1]);
            } catch (ArgumentException e) {
                throw new SecurityException(e.Message);
            }

            // All done
            return new BlobStorageEntityUri(bucket: bucketName, key: components[2]);
        }

        /* Bucket names must start with a letter */
        static Regex BUCKET_NAME_REGEX = new Regex(@"^[a-z][a-z0-9_\-]*$");

        private string NormalizeBucketName(string name) {
            var lowerName = name.ToLowerInvariant();
            var match = BUCKET_NAME_REGEX.Match(lowerName);
            if (match.Success) {
                return lowerName;
            } else {
                throw new ArgumentException("invalid bucket name");
            }
        }
    }
}