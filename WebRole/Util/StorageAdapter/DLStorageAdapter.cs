using Microsoft.Azure.Management.DataLake.Store;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ChumBucket.Util.DataLake;
using ChumBucket.Util.Uris;
using WebRole.Util.StorageAdapter;

namespace ChumBucket.Util.Storage {
    /**
     * An adapter for communicating with Data Lake storage.
     */
    public class DLStorageAdapter : IStorageAdapter {
        private readonly DLClient _client;
        private readonly IFileSystemOperations _fs;

        /**
         * The "container" name.
         */
        private readonly string _containerName;

        /**
         * A simple JSON serializable object for upload metadata
         */
        private class Meta {
            [JsonProperty("name")]
            public string Name { get; set; }
            [JsonProperty("guid")]
            public string Guid { get; set; }
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
            if (file.ContentType != "text/csv") {
                throw new ArgumentException("content type must be text/csv");
            } else {
                ValidationHelpers.ValidateBucketName(bucket);
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
                Guid = Guid.NewGuid().ToString(),
                ContentType = file.ContentType
            });

            var metaStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
            this._fs.Append(this._client.AccountName, metaPath, metaStream);
        }

        public StorageFile Retrieve(EntityUri uri) {
            try {
                ValidationHelpers.ValidateUri(uri, typeof(DLEntityUri));

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
                    var metadata = ReadMeta(meta);

                    return new StorageFile(upload, uri, metadata.Name, metadata.ContentType);
                }
            } catch (Exception e) when (e is JsonException) {
                throw new KeyNotFoundException(e.Message);
            }
        }

        public void Delete(EntityUri uri) {
            ValidationHelpers.ValidateUri(uri, typeof(DLEntityUri));

            var bucket = uri.Bucket;
            var key = Path.GetFileNameWithoutExtension(uri.Key);
            var uploadPath = this.UploadPath(bucket, key);
            var metaPath = this.MetaPath(bucket, key);

            if (!this._fs.PathExists(this._client.AccountName, uploadPath) ||
                !this._fs.PathExists(this._client.AccountName, metaPath)) {
                throw new KeyNotFoundException("key does not exist");
            } else {
                this._fs.Delete(this._client.AccountName, uploadPath);
                this._fs.Delete(this._client.AccountName, metaPath);
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
            ValidationHelpers.ValidateBucketName(bucket);

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

        /**
         * Returns the path for the specified bucket
         * <param name="bucket">The bucket</param>
         * <returns>The absolute path, as a string</returns>
         */
        private string BucketPath(string bucket) {
            return $"{this._containerName}/{bucket}";
        }

        /**
         * Returns a path to the uploaded file
         * <param name="bucket">The bucket</param>
         * <param name="key">The key</param>
         * <returns>The absolute path, as a string</returns>
         */
        private string UploadPath(string bucket, string key) {
            return this.AbsPath(bucket, key, "csv");
        }

        /**
         * Returns a path to the meta file
         * <param name="bucket">The bucket</param>
         * <param name="key">The key</param>
         * <returns>The absolute path, as a string</returns>
         */
        private string MetaPath(string bucket, string key) {
            return this.AbsPath(bucket, key, "json");
        }

        /**
         * Returns an absolute path given a bucket, key, and extension.
         * <param name="bucket">The bucket</param>
         * <param name="key">The key</param>
         * <param name="extension">The extension</param>
         * <returns>The absolute path, as a string</returns>
         */
        private string AbsPath(string bucket, string key, string extension) {
            return string.Format(
                "{0}/{1}/{2}.{3}",
                this._containerName, bucket,
                Path.GetFileNameWithoutExtension(key), extension
            );
        }

        /**
         * Creates a directory if it doesn't exist.
         * <param name="path">The path</param>
         */
        private void Mkdir(string path) {
            if (!this._fs.PathExists(this._client.AccountName, path)) {
                this._fs.Mkdirs(this._client.AccountName, path);
            }
        }

        /**
         * Reads metadata from a file's meta stream
         * <param name="stream">The stream</param>
         * <returns>An instance of Meta</returns>
         */
        private static Meta ReadMeta(Stream stream) {
            using (var metaReader = new StreamReader(stream)) {
                var json = metaReader.ReadToEnd();
                return JsonConvert.DeserializeObject<Meta>(json);
            }
        }
    }
}