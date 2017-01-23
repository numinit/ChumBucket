using System;

namespace ChumBucket.Util.Uris {
    /**
     * A factory interface for building direct URIs to resources.
     */
    public interface IDirectUriFactory {
        /**
         * Builds a direct URI to a resource given a bucket and a key.
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        Uri BuildDirectUri(string bucket, string key = null);

        /**
         * Builds a direct HTTPS URI to a resource given a bucket and a key.
         * <param name="bucket">The bucket name</param>
         * <param name="key">The key</param>
         */
        Uri BuildDirectHttpsUri(string bucket, string key = null);
    }
}