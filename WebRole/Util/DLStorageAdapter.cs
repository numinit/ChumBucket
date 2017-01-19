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
using WebRole.Util;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Text;

namespace ChumBucket.Util
{
    public class DLStorageAdapter : StorageAdapter {
        private DLClient _client;
        private IFileSystemOperations _fs;

        /**
         * The "container" name.
         */
        private string _containerName;

        /**
         * The authority for URIs produced by this instance
         */
        private string _authority;

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

            // The authority for this adapter is <account name>.azuredatalake.net
            this._authority = string.Format("{0}.azuredatalake.net", this._client.AccountName);

            // Make job control directories
            if (!this._fs.PathExists(this._client.AccountName, containerName)) {
                this._fs.Mkdirs(this._client.AccountName, containerName);
            }
        }

        public override Uri Store(StorageFile file, Guid guid) {
            if (!file.ContentType.Equals("text/csv")) {
                throw new ArgumentException("content type must be text/csv");
            }

            // Create and open the upload and meta files
            var uploadPath = this.UploadPath(guid);
            var metaPath = this.MetaPath(guid);
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
            
            return this.BuildUri(guid);
        }

        public override StorageFile Retrieve(Uri uri) {
            this.AssertAccepts(uri);

            try {
                // Strip the leading slash from the URI's path to get the GUID
                var guid = Guid.Parse(Path.GetFileNameWithoutExtension(uri.AbsolutePath));
                var uploadPath = this.UploadPath(guid);
                var metaPath = this.MetaPath(guid);

                if (!this._fs.PathExists(this._client.AccountName, uploadPath) ||
                    !this._fs.PathExists(this._client.AccountName, metaPath)) {
                    throw new KeyNotFoundException(string.Format("file {0} does not exist", guid.ToString()));
                } else {
                    var upload = this._fs.Open(this._client.AccountName, uploadPath);
                    var meta = this._fs.Open(this._client.AccountName, metaPath);
                    var metadata = this.ReadMeta(meta);

                    return new StorageFile(upload, metadata.Name, metadata.ContentType);
                }
            } catch (Exception e) when (e is ArgumentNullException || e is FormatException || e is JsonException) {
                throw new ArgumentException(e.Message);
            }
        }

        public override ICollection<StorageFile> List() {
            var ret = new List<StorageFile>();
            var regex = new Regex(@"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json$");
            var statuses = this._fs.ListFileStatus(this._client.AccountName, this._containerName);
            foreach (var status in statuses.FileStatuses.FileStatus) {
                try {
                    var name = status.PathSuffix;
                    if (status.Type == Microsoft.Azure.Management.DataLake.Store.Models.FileType.FILE) {
                        var match = regex.Match(name);
                        if (match.Success) {
                            // This is the metadata file, and the prefix is the GUID.
                            var guid = Guid.Parse(match.Groups[1].Value);
                            var upload = this._fs.Open(this._client.AccountName, this.UploadPath(guid));
                            var meta = this._fs.Open(this._client.AccountName, this.MetaPath(guid));
                            var metadata = this.ReadMeta(meta);

                            ret.Add(new StorageFile(upload, metadata.Name, metadata.ContentType));
                        }
                    }
                } catch (Exception e) when (e is ArgumentException || e is FormatException || e is JsonException) {
                    continue;
                }
            }
            return ret;
        }

        public override bool WillAccept(Uri uri) {
            return uri.Scheme.Equals("adl") &&
                   uri.Authority.Equals(this._authority);
        }

        public override Uri BuildUri(Guid guid) {
            // adl://chumbucket.azuredatalake.net/{contaner}/{guid}.csv
            var builder = new UriBuilder();
            builder.Scheme = "adl";
            builder.Host = this._authority;
            builder.Path = string.Format("/{0}", this.UploadPath(guid));
            return builder.Uri;
        }

        private string UploadPath(Guid guid) {
            return string.Format("{0}.csv", Path.Combine(this._containerName, guid.ToString()));
        }

        private string MetaPath(Guid guid) {
            return string.Format("{0}.json", Path.Combine(this._containerName, guid.ToString()));
        }

        private Meta ReadMeta(Stream stream) {
            using (var metaReader = new StreamReader(stream)) {
                var json = metaReader.ReadToEnd();
                return JsonConvert.DeserializeObject<Meta>(json);
            }
        }
    }
}