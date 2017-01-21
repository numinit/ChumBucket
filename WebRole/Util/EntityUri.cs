using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;

namespace ChumBucket.Util {
    public abstract class EntityUri : Uri {
        private static Regex CHUMBUCKET_PLUS = new Regex(@"^chumbucket\+(.+)$");

        public enum UriScope {
            BUCKET = 0,
            FILE = 1
        }

        public string Namespace {
            get { return this.GetNamespace(); }
        }

        public UriScope Scope {
            get {
                if (this.AbsolutePath == "/") {
                    return UriScope.BUCKET;
                } else {
                    return UriScope.FILE;
                }
            }
        }

        public string Bucket {
            get {
                return this.Authority;
            }
        }

        public string Key {
            get {
                if (this.Scope == UriScope.BUCKET) {
                    return null;
                } else {
                    var path = this.AbsolutePath;
                    return Path.GetFileName(path);
                }
            }
        }

        public EntityUri(string ns = null, string uri = null, string bucket = null, string key = null)
            : base(BuildUri(ns, uri, bucket, key)) {
            var match = CHUMBUCKET_PLUS.Match(this.Scheme);
            if (match.Success) {
                this.ValidateNamespace(match.Groups[1].Value);
            } else {
                throw new ArgumentException("invalid scheme");
            }
        }

        /**
         * Validates that the namespace is correct
         */
        private void ValidateNamespace(string ns) {
            if (ns != this.GetNamespace()) {
                throw new ArgumentException("invalid namespace");
            }
        }

        /**
         * Returns the correct namespace for this URI
         * <returns>The proper namespace</returns>
         */
        public abstract string GetNamespace();

        /**
         * Returns a direct URI to this asset
         * <param name="accountName">The account name</param>
         * <param name="containerName">The container name</param>
         * <returns>A direct URI to the resource</returns>
         */
        public abstract Uri ToDirectUri(string accountName, string containerName);

        /**
         * Returns a direct HTTPS URI to this asset
         * <param name="accountName">The account name</param>
         * <param name="containerName">The container name</param>
         * <returns>A direct HTTPS URI to the resource</returns>
         */
        public abstract Uri ToDirectHttpsUri(string accountName, string containerName);

        public static EntityUri FromBlobStorage(string bucket, string key = null) {
            return new BlobStorageEntityUri(bucket: bucket, key: key);
        }

        public static EntityUri FromDataLake(string bucket, string key = null) {
            return new DLEntityUri(bucket: bucket, key: key);
        }

        protected static string BuildUri(string ns, string uri, string bucket, string key) {
            if (uri != null) {
                return uri;
            } else if (ns != null && bucket != null) {
                UriBuilder builder = new UriBuilder();
                builder.Scheme = string.Format("chumbucket+{0}", ns);
                builder.Host = bucket;
                if (key != null) {
                    builder.Path = string.Format("/{0}", key);
                }
                return builder.Uri.ToString();
            } else {
                throw new ArgumentNullException("must provide either URI or namespace and bucket");
            }
        }
    }

    public class BlobStorageEntityUri : EntityUri {
        private static string NS = "wasb";

        public BlobStorageEntityUri(string uri = null, string bucket = null, string key = null) : base(NS, uri, bucket, key) { }

        public override string GetNamespace() {
            return NS;
        }

        public override Uri ToDirectUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder();
            builder.Scheme = this.Namespace;
            builder.Host = string.Format("{0}@{1}", containerName, accountName);
            if (this.Scope == UriScope.BUCKET) {
                builder.Path = string.Format("/{0}", this.Authority);
            } else {
                builder.Path = string.Format("/{0}{1}", this.Authority, this.AbsolutePath);
            }
            return builder.Uri;
        }

        public override Uri ToDirectHttpsUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder();
            builder.Scheme = "https";
            builder.Host = string.Format("{0}.blob.core.windows.net", accountName);
            if (this.Scope == UriScope.BUCKET) {
                builder.Path = string.Format("/{0}/{1}", containerName, this.Authority);
            } else {
                builder.Path = string.Format("/{0}/{1}{2}", containerName, this.Authority, this.AbsolutePath);
            }
            return builder.Uri;
        }
    }

    public class DLEntityUri : EntityUri {
        private static string NS = "adl";
         
        public DLEntityUri(string uri = null, string bucket = null, string key = null) : base(NS, uri, bucket, key) { }

        public override string GetNamespace() {
            return NS;
        }

        public override Uri ToDirectUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder();
            builder.Scheme = this.Namespace;
            builder.Host = string.Format("{0}.azuredatalake.net", accountName);
            if (this.Scope == UriScope.BUCKET) {
                builder.Path = string.Format("/{0}/{1}", containerName, this.Authority);
            } else {
                builder.Path = string.Format("/{0}/{1}{2}", containerName, this.Authority, this.AbsolutePath);
            }
            return builder.Uri;
        }

        public override Uri ToDirectHttpsUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder();
            builder.Scheme = "https";
            builder.Host = string.Format("{0}.azuredatalakestore.net", accountName);
            if (this.Scope == UriScope.BUCKET) {
                builder.Path = string.Format("/webhdfs/v1/{0}/{1}", containerName, this.Authority);
            } else {
                builder.Path = string.Format("/webhdfs/v1/{0}/{1}{2}", containerName, this.Authority, this.AbsolutePath);
            }
            return builder.Uri;
        }
    }

    public class JobEntityUri : EntityUri {
        private static string NS = "job";

        public JobEntityUri(string uri = null, string bucket = null, string key = null) : base(NS, uri, bucket, key) { }

        public override string GetNamespace() {
            return NS;
        }

        public override Uri ToDirectUri(string accountName, string containerName) {
            return this;
        }

        public override Uri ToDirectHttpsUri(string accountName, string containerName) {
            throw new NotImplementedException();
        }
    }
}