using ChumBucket.Util.Storage;
using System;

namespace ChumBucket.Util.Uris {
    /**
     * Builds direct blob storage URIs from bucket and key names.
     */
    public class BlobStorageUriFactory : IDirectUriFactory {
        private readonly IStorageAdapter _adapter;

        /**
         * Initializes this BlobStorageUriFactory.
         * <param name="adapter">The supporting storage adapter</param>
         */
        public BlobStorageUriFactory(IStorageAdapter adapter) {
            this._adapter = adapter;
        }

        /**
         * <see cref="IDirectUriFactory"/>
         */
        public override Uri BuildDirectUri(string bucket, string key) {
            var entityUri = new BlobStorageEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectUri(this._adapter.GetAccountName(),
                                         this._adapter.GetContainerName());
        }

        /**
         * <see cref="IDirectUriFactory"/>
         */
        public override Uri BuildDirectHttpsUri(string bucket, string key) {
            var entityUri = new BlobStorageEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectHttpsUri(this._adapter.GetAccountName(),
                                              this._adapter.GetContainerName());
        }
    }
}
 