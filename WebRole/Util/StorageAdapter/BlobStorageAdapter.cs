using ChumBucket.Util.Uris;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using System.Text.RegularExpressions;
using WebRole.Util.StorageAdapter;

namespace ChumBucket.Util.Storage {
    /**
     * An adapter for Azure Blob Storage.
     */
    public class BlobStorageAdapter : IStorageAdapter {
        private readonly IDirectUriFactory _factory;
        private readonly CloudStorageAccount _storageAccount;
        private readonly CloudBlobClient _blobClient;
        private readonly CloudBlobContainer _blobContainer;

        /**
         * Initializes this BlobStorageAdapter.
         * <param name="connectionString">The connection string</param>
         * <param name="containerName">The container name</param>
         */
        public BlobStorageAdapter(string connectionString, string containerName) {
            this._factory = new BlobStorageUriFactory(this);
            this._storageAccount = CloudStorageAccount.Parse(connectionString);
            this._blobClient = this._storageAccount.CreateCloudBlobClient();
            this._blobContainer = this._blobClient.GetContainerReference(containerName);
            this._blobContainer.CreateIfNotExists();
        }

        public override string GetAccountName() {
            return this._blobClient.Credentials.AccountName;
        }

        public override string GetContainerName() {
            return this._blobContainer.Name;
        }

        public override void Store(StorageFile file, string bucket) {
            ValidationHelpers.ValidateBucketName(bucket);

            var directory = this._blobContainer.GetDirectoryReference(bucket);
            var blob = directory.GetBlockBlobReference(file.Name);
            using (file.InputStream) {
                blob.Metadata["name"] = file.Name;
                blob.Properties.ContentType = file.ContentType;
                blob.UploadFromStream(file.InputStream);
            }
        }

        public override StorageFile Retrieve(EntityUri uri) {
            ValidationHelpers.ValidateUri(uri, typeof(BlobStorageEntityUri));

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
        }

        public override void Delete(EntityUri uri) {
            ValidationHelpers.ValidateUri(uri, typeof(BlobStorageEntityUri));

            var directory = this._blobContainer.GetDirectoryReference(uri.Bucket);
            var key = uri.Key;
            var blob = directory.GetBlockBlobReference(key);

            var exists = blob.DeleteIfExists();
            if (!exists) {
                throw new KeyNotFoundException("key does not exist");
            }
        }

        public override ICollection<EntityUri> ListBuckets() {
            var list = new List<EntityUri>();
            var blobs = this._blobContainer.ListBlobs();
            var buckets = blobs.Where(b => b is CloudBlobDirectory);
            foreach (var bucket in buckets) {
                list.Add(this.GetBlobStorageEntityUriFromDirect(bucket.Uri, bucketsOnly: true));
            }
            return list;
        }

        public override ICollection<EntityUri> ListFiles(string bucket) {
            ValidationHelpers.ValidateBucketName(bucket);
            var list = new List<EntityUri>();
            var directory = this._blobContainer.GetDirectoryReference(bucket);
            var blobs = directory.ListBlobs().Where(b => b is CloudBlockBlob);
            foreach (var blob in blobs) {
                list.Add(this.GetBlobStorageEntityUriFromDirect(blob.Uri));
            }
            return list;
        }

        /**
         * Finalizes the uploaded blob, assigning it a GUID,
         * and setting its content type to the specified type.
         * <param name="directUri">The blob URI</param>
         * <param name="name">The filename</param>
         * <param name="contentType">The content type, or null for application/octet-stream</param>
         * <returns>A BlobStorageEntityUri for the blob</returns>
         */
        public BlobStorageEntityUri FinalizeUploadedBlob(Uri blobUri, string name, string contentType) {
            var entityUri = this.GetBlobStorageEntityUriFromDirect(blobUri);
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

        /**
         * Returns the root HTTPS direct URI
         * <returns>The root URI</returns>
         */
        public Uri GetRootUri() {
            return this._factory.BuildDirectHttpsUri("");
        }

        /**
         * Returns a SAS (shared access signature) URI for a given direct URI.
         * If there is an issue with the given blob URI, throws a SecurityException.
         * <param name="directUri">The direct URI</param>
         * <param name="verb">The HTTP verb</param>
         * <param name="allowedVerbs">The allowed HTTP verbs</param>
         * <param name="expirationMins">The expiration time, in minutes. Default is 15.</param>
         * <returns>A SAS URI</returns>
         */
        public Uri GetSasForBlob(Uri directUri, string verb, string[] allowedVerbs, int expirationMins = 15) {
            var entityUri = this.GetBlobStorageEntityUriFromDirect(directUri);
            var bucket = this._blobContainer.GetDirectoryReference(entityUri.Bucket);
            var blob = bucket.GetBlockBlobReference(entityUri.Key);
            verb = verb.ToUpperInvariant();
            if (!allowedVerbs.Contains(verb)) {
                throw new SecurityException("unauthorized HTTP verb");
            }

            SharedAccessBlobPermissions permission;
            switch (verb) {
                case "GET":
                    permission = SharedAccessBlobPermissions.Read;
                    break;
                case "PUT":
                    permission = SharedAccessBlobPermissions.Write;
                    break;
                case "DELETE":
                    permission = SharedAccessBlobPermissions.Delete;
                    break;
                default:
                    throw new SecurityException("unsupported HTTP verb");
            }

            // Expire their token after some time
            var sas = blob.GetSharedAccessSignature(new SharedAccessBlobPolicy() {
                Permissions = permission,
                SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(expirationMins),
            });

            var rewrittenUri = this._factory.BuildDirectHttpsUri(entityUri.Bucket, entityUri.Key);
            return new Uri($"{rewrittenUri}{sas}");
        }

        /**
         * Returns a direct SAS (shared access signature) URI for a given blob URI.
         * If there is an issue with the given blob URI, throws a SecurityException.
         * <param name="directUri">The blob URI</param>
         * <param name="verb">The HTTP verb</param>
         * <param name="allowedVerbs">The allowed HTTP verbs</param>
         * <param name="expirationMins">The expiration time, in minutes. Default is 15.</param>
         * <returns>A SAS URI</returns>
         */
        public Uri GetSasForBlob(BlobStorageEntityUri blobUri, string verb, string[] allowedVerbs, int expirationMins = 15) {
            return this.GetSasForBlob(this._factory.BuildDirectHttpsUri(blobUri.Bucket, blobUri.Key), verb, allowedVerbs, expirationMins);
        }

        private static readonly Regex BLOB_CORE_REGEX = new Regex(@"^(?<account>[^\.]+)\.blob\.core\.windows\.net$", RegexOptions.Compiled);

        /**
         * Gets a blob storage entity URI from a direct URI.
         * If there's a mismatch between the direct URI and our configuration,
         * throws a SecurityException.
         * <param name="directUri">The blob URI</param>
         * <param name="bucketsOnly">Whether to only allow bucket URIs</param>
         * <returns>The corresponding BlobStorageEntityUri</returns>
         */
        public BlobStorageEntityUri GetBlobStorageEntityUriFromDirect(Uri blobUri, bool bucketsOnly = false) {
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

            // Remove the leading slash and any trailing slashes
            var path = blobUri.AbsolutePath.TrimStart('/').TrimEnd('/');
            var components = path.Split('/');
            if (bucketsOnly && components.Length != 2 ||
                !bucketsOnly && components.Length != 3) {
                throw new SecurityException("invalid number of path components");
            } else if (components[0] != this._blobContainer.Name) {
                throw new SecurityException("invalid container name");
            }

            // Normalize the bucket name
            string bucketName = components[1];
            try {
                ValidationHelpers.ValidateBucketName(bucketName);
            } catch (ArgumentException e) {
                throw new SecurityException(e.Message);
            }

            // All done
            if (bucketsOnly) {
                return new BlobStorageEntityUri(bucket: bucketName);
            } else {
                return new BlobStorageEntityUri(bucket: bucketName, key: components[2]);
            }
        }
    }
}