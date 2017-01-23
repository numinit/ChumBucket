using System;
using System.Text.RegularExpressions;
using ChumBucket.Util.Uris;

namespace WebRole.Util.StorageAdapter {
    public class ValidationHelpers {
        /**
         * Validates that the specified URI references a single key
         * and is a subclass of `klass`
         * <param name="uri">The URI</param>
         * <param name="klass">The class</param>
         */
        public static void ValidateUri(EntityUri uri, Type klass) {
            if (!klass.IsInstanceOfType(uri)) {
                throw new ArgumentException($"URI must be of type {klass}");
            } else if (uri.Scope != EntityUri.UriScope.Key) {
                throw new ArgumentException("URI must reference a single key");
            } else {
                ValidateBucketName(uri.Bucket);
            }
        }

        /* Bucket names must start with a letter */
        private static readonly Regex BUCKET_NAME_REGEX = new Regex(@"^[a-z][a-z0-9_\-]*$", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        /**
         * Validates that the specified bucket name is normal.
         * <param name="name">The bucket name</param>
         */
        public static void ValidateBucketName(string name) {
            var match = BUCKET_NAME_REGEX.Match(name);
            if (!match.Success) {
                throw new ArgumentException("invalid bucket name");
            }
        }
    }
}