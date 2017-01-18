using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Web;

namespace ChumBucket.Util {
    public class BlobStorageAdapter : StorageAdapter {
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;
        private CloudBlobContainer _blobContainer;
        private string _authority;

        public BlobStorageAdapter(string connectionString, string containerName) {
            this._storageAccount = CloudStorageAccount.Parse(connectionString);
            this._blobClient = this._storageAccount.CreateCloudBlobClient();
            this._blobContainer = this._blobClient.GetContainerReference(containerName);
            this._blobContainer.CreateIfNotExists();

            // Create the authority part of the URI: container@account
            this._authority = string.Format("{0}@{1}", containerName, this._storageAccount.Credentials.AccountName);
        }

        public override Uri Store(StorageFile file, Guid guid) {
            var key = guid.ToString();
            var blob = this._blobContainer.GetBlockBlobReference(key);
            using (file.InputStream) {
                blob.Metadata["name"] = file.Name;
                blob.Properties.ContentType = file.ContentType;
                blob.UploadFromStream(file.InputStream);
            }
            return this.BuildUri(guid);
        }

        public override StorageFile Retrieve(Uri uri) {
            this.AssertAccepts(uri);

            try {
                // The GUID is in the URI's path; strip the leading slash
                var guid = Guid.Parse(uri.AbsolutePath.Substring(1));
                var key = guid.ToString();

                // Find a reference to the blob
                var blob = this._blobContainer.GetBlockBlobReference(key);
                if (!blob.Exists()) {
                    throw new KeyNotFoundException(string.Format("blob {0} does not exist", key));
                } else {
                    var stream = blob.OpenRead();
                    var name = blob.Metadata["name"];
                    if (name == null) {
                        // No other name stored
                        name = key;
                    }

                    var contentType = this.contentTypeOrDefault(blob.Properties.ContentType);
                    return new StorageFile(stream, name, contentType);
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException) {
                throw new ArgumentException(e.Message);
            }
        }

        public override ICollection<StorageFile> List() {
            var ret = new List<StorageFile>();
            var blobs = this._blobContainer.ListBlobs(useFlatBlobListing: true);
            foreach (IListBlobItem item in blobs) {
                var blob = this._blobClient.GetBlobReferenceFromServer(item.Uri);
                var name = blob.Metadata["name"];
                if (name == null) {
                    name = blob.Name;
                }
                var contentType = this.contentTypeOrDefault(blob.Properties.ContentType);
                ret.Add(new StorageFile(blob.OpenRead(), name, contentType));
            }
            return ret;
        }

        public override bool WillAccept(Uri uri) {
            // wasb://<user>@<host>
            return uri.Scheme.Equals("wasb") &&
                   string.Format("{0}@{1}", uri.UserInfo, uri.Host).Equals(this._authority);
        }

        public override Uri BuildUri(Guid guid) {
            var builder = new UriBuilder();
            builder.Scheme = "wasb";
            builder.Host = this._authority;
            builder.Path = string.Format("/{0}", guid.ToString());
            return builder.Uri;
        }

        private string contentTypeOrDefault(string contentType) {
            if (contentType != null) {
                return contentType;
            } else {
                return "application/octet-stream";
            }
        }
    }
}