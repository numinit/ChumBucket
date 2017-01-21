using System;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.Azure.Management.DataLake.StoreUploader;
using Microsoft.Azure.Management.DataLake.Analytics;
using Microsoft.Azure.Management.DataLake.Analytics.Models;
using ChumBucket.Util;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Text;

namespace ChumBucket.Util {
    public class DLStorageAdapter : IStorageAdapter {
        private DLClient _client;
        private IFileSystemOperations _fs;

        /**
         * The "container" name.
         */
        private string _containerName;

        /**
         * A simple JSON serializable object for upload metadata
         */
        private class Meta {
            [JsonProperty("name")]
            public string Name { get; set; }
            [JsonProperty("contentType")]
            public string ContentType { get; set; }
        }

        public DLStorageAdapter(DLClient client, string containerName) {
            this._client = client;
            this._fs = this._client.FsClient.FileSystem;
            this._containerName = containerName;

            // Make the container
            this.Mkdir(containerName);
        }

        public string GetAccountName() {
            return this._client.AccountName;
        }

        public string GetContainerName() {
            return this._containerName;
        }

        public void Store(StorageFile file, string bucket) {
            if (!file.ContentType.Equals("text/csv")) {
                throw new ArgumentException("content type must be text/csv");
            }

            // Create and open the upload and meta files
            var bucketPath = this.BucketPath(bucket);
            var uploadPath = this.UploadPath(bucket, file.Name);
            var metaPath = this.MetaPath(bucket, file.Name);

            // Create the bucket if it doesn't exist
            this.Mkdir(bucketPath);

            // Create the upload and meta files
            this._fs.Create(this._client.AccountName, uploadPath);
            this._fs.Create(this._client.AccountName, metaPath);

            // Write the file
            this._fs.ConcurrentAppend(this._client.AccountName, uploadPath, file.InputStream);

            // Write the metadata
            var json = JsonConvert.SerializeObject(new Meta {
                Name = file.Name,
                ContentType = file.ContentType
            });

            var metaStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            this._fs.Append(this._client.AccountName, metaPath, metaStream);
        }

        public StorageFile Retrieve(EntityUri uri) {
            try {
                // Strip the leading slash from the URI's path to get the GUID
                var bucket = uri.Bucket;
                var key = Path.GetFileNameWithoutExtension(uri.Key);
                var uploadPath = this.UploadPath(bucket, key);
                var metaPath = this.MetaPath(bucket, key);

                if (!this._fs.PathExists(this._client.AccountName, uploadPath) ||
                    !this._fs.PathExists(this._client.AccountName, metaPath)) {
                    throw new KeyNotFoundException("key does not exist");
                } else {
                    var upload = this._fs.Open(this._client.AccountName, uploadPath);
                    var meta = this._fs.Open(this._client.AccountName, metaPath);
                    var metadata = this.ReadMeta(meta);

                    return new StorageFile(upload, uri, metadata.Name, metadata.ContentType);
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException || e is JsonException) {
                throw new ArgumentException(e.Message);
            }
        }

        public ICollection<EntityUri> ListBuckets() {
            var ret = new List<EntityUri>();
            var statuses = this._fs.ListFileStatus(this._client.AccountName, this._containerName);
            foreach (var status in statuses.FileStatuses.FileStatus) {
                if (status.Type == Microsoft.Azure.Management.DataLake.Store.Models.FileType.DIRECTORY) {
                    ret.Add(new DLEntityUri(bucket: Path.GetFileName(status.PathSuffix)));
                }
            }
            return ret;
        }

        public ICollection<EntityUri> ListFiles(string bucket) {
            var ret = new List<EntityUri>();
            var statuses = this._fs.ListFileStatus(this._client.AccountName, this.BucketPath(bucket));

            foreach (var status in statuses.FileStatuses.FileStatus) {
                if (status.Type == Microsoft.Azure.Management.DataLake.Store.Models.FileType.FILE &&
                    status.PathSuffix.EndsWith(".csv")) {
                    ret.Add(new DLEntityUri(bucket: bucket, key: Path.GetFileName(status.PathSuffix)));
                }
            }

            return ret;
        }

        private string BucketPath(string bucket) {
            return Path.Combine(this._containerName, bucket);
        }

        private string UploadPath(string bucket, string key) {
            return this.AbsPath(bucket, key, "csv");
        }

        private string MetaPath(string bucket, string key) {
            return this.AbsPath(bucket, key, "json");
        }

        private string AbsPath(string bucket, string key, string extension) {
            return Path.Combine(
                this.BucketPath(bucket),
                string.Format("{0}.{1}", Path.GetFileNameWithoutExtension(key), extension)
            );
        }

        private void Mkdir(string path) {
            if (!this._fs.PathExists(this._client.AccountName, path)) {
                this._fs.Mkdirs(this._client.AccountName, path);
            }
        }

        private Meta ReadMeta(Stream stream) {
            using (var metaReader = new StreamReader(stream)) {
                var json = metaReader.ReadToEnd();
                return JsonConvert.DeserializeObject<Meta>(json);
            }
        }
    }
}