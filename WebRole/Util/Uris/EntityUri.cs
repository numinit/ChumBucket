using System;
using System.IO;
using System.Text.RegularExpressions;

namespace ChumBucket.Util.Uris {
    /**
     * An EntityUri is a serializable pointer to an object
     * in the ChumBucket system. Entity URIs are designed to
     * remove the burden of dealing with low-level URIs for
     * storage locations such as Azure Data Lake Store and
     * Azure Blob Storage.
     * 
     * For example, an entity URI for an object stored in Blob
     * Storage may be "chumbucket+wasb://bucket/key". If the
     * path component of the URI is missing, the URI refers to a
     * bucket. Otherwise, it refers to a key. This is referred to
     * as the "scope" of an entity URI.
     */
    public abstract class EntityUri : Uri {
        /**
         * The prefix for all ChumBucket URIs
         */
        private static readonly Regex CHUMBUCKET_PLUS = new Regex(@"^chumbucket\+(.+)$", RegexOptions.Compiled);

        /**
         * The scope of this entity URI.
         * Bucket scope: this URI points to a bucket
         * File scope: this URI points to a file
         */
        public enum UriScope {
            Bucket = 0,
            Key = 1
        }

        /**
         * Returns the namespace of this URI; that is,
         * the part of the scheme after "chumbucket+".
         */
        public string Namespace => this.GetNamespace();

        /**
         * Returns the scope of this URI; that is,
         * whether it points to a bucket or a file.
         */
        public UriScope Scope => this.AbsolutePath == "/" ? UriScope.Bucket : UriScope.Key;

        /**
         * Returns the bucket for this URI.
         */
        public string Bucket => this.Authority;

        /**
         * Returns the key for this entity URI.
         * If this URI has bucket scope, returns null.
         */
        public string Key {
            get {
                if (this.Scope == UriScope.Bucket) {
                    return null;
                } else {
                    var path = this.AbsolutePath;
                    return Path.GetFileName(path);
                }
            }
        }

        /**
         * Initializes this EntityUri.
         * <param name="ns">The namespace</param>
         * <param name="uri">The URI, which will be parsed, overriding the bucket and key.</param>
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        protected EntityUri(string ns = null, string uri = null, string bucket = null, string key = null)
            : base(BuildUri(ns, uri, bucket, key)) {
            var match = CHUMBUCKET_PLUS.Match(this.Scheme);
            if (match.Success) {
                this.ValidateNamespace(match.Groups[1].Value);
            } else {
                throw new ArgumentException(
                    $"invalid URI scheme `{this.Scheme}`; expected `chumbucket+{this.GetNamespace()}`");
            }
        }

        /**
         * Validates that the namespace is correct
         * <param name="ns">The namespace to validate</param>
         */
        private void ValidateNamespace(string ns) {
            if (ns != this.GetNamespace()) {
                throw new ArgumentException($"invalid namespace `{ns}`; expected `{this.GetNamespace()}`");
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

        /**
         * Builds a URI.
         * <param name="ns">The desired namespace</param>
         * <param name="uri">The URI, or null if we should build a URI from a bucket and key</param>
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        protected static string BuildUri(string ns, string uri, string bucket, string key) {
            if (uri != null) {
                return uri;
            } else if (ns != null && bucket != null) {
                UriBuilder builder = new UriBuilder {
                    Scheme = $"chumbucket+{ns}",
                    Host = bucket
                };

                if (key != null) {
                    builder.Path = $"/{key}";
                }
                return builder.Uri.ToString();
            } else {
                throw new ArgumentException("must provide either URI or namespace and bucket");
            }
        }
    }

    /**
     * Represents a pointer to an object in Azure Blob Storage.
     */
    public class BlobStorageEntityUri : EntityUri {
        /**
         * The namespace for Azure Blob Storage URIs
         */
        private static readonly string NS = "wasb";

        /**
         * Initializes this BlobStorageEntityUri.
         * <see cref="EntityUri"/>
         */
        public BlobStorageEntityUri(string uri = null, string bucket = null, string key = null) : base(NS, uri, bucket, key) { }

        public override string GetNamespace() {
            return NS;
        }

        public override Uri ToDirectUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder {
                Scheme = this.Namespace,
                Host = $"{containerName}@{accountName}"
            };
            if (this.Scope == UriScope.Bucket) {
                builder.Path = $"/{this.Authority}";
            } else {
                builder.Path = $"/{this.Authority}{this.AbsolutePath}";
            }
            return builder.Uri;
        }

        public override Uri ToDirectHttpsUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder {
                Scheme = "https",
                Host = $"{accountName}.blob.core.windows.net"
            };
            if (this.Scope == UriScope.Bucket) {
                builder.Path = $"/{containerName}/{this.Authority}";
            } else {
                builder.Path = $"/{containerName}/{this.Authority}{this.AbsolutePath}";
            }
            return builder.Uri;
        }
    }

    /**
     * Represents a pointer to an object in Azure Data Lake.
     */
    public class DLEntityUri : EntityUri {
        /**
         * The namespace for Azure Data Lake URIs
         */
        private static readonly string NS = "adl";

        /**
         * Initializes this DLEntityUri.
         * <see cref="EntityUri"/>
         */
        public DLEntityUri(string uri = null, string bucket = null, string key = null) : base(NS, uri, bucket, key) { }

        public override string GetNamespace() {
            return NS;
        }

        public override Uri ToDirectUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder {
                Scheme = this.Namespace,
                Host = $"{accountName}.azuredatalake.net"
            };
            if (this.Scope == UriScope.Bucket) {
                builder.Path = $"/{containerName}/{this.Authority}";
            } else {
                builder.Path = $"/{containerName}/{this.Authority}{this.AbsolutePath}";
            }
            return builder.Uri;
        }

        public override Uri ToDirectHttpsUri(string accountName, string containerName) {
            UriBuilder builder = new UriBuilder {
                Scheme = "https",
                Host = $"{accountName}.azuredatalakestore.net"
            };
            if (this.Scope == UriScope.Bucket) {
                builder.Path = $"/webhdfs/v1/{containerName}/{this.Authority}";
            } else {
                builder.Path = $"/webhdfs/v1/{containerName}/{this.Authority}{this.AbsolutePath}";
            }
            return builder.Uri;
        }
    }

    public class JobEntityUri : EntityUri {
        /**
         * The namespace for ChumBucket jobs
         */
        private static readonly string NS = "job";

        /**
         * Initializes this JobEntityUri.
         * <see cref="EntityUri"/>
         */
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