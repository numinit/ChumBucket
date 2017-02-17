using System;

namespace ChumBucket.Util.Uris {
    /**
     * A factory interface for building direct URIs to resources.
     */

    public abstract class IDirectUriFactory {
        public abstract Uri BuildDirectUri(string bucket, string key = null);
        public abstract Uri BuildDirectHttpsUri(string bucket, string key = null);
    }
}