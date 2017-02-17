using ChumBucket.Util.Storage;
using System;

namespace ChumBucket.Util.Uris {
    /**
     * Builds direct Data Lake storage URIs from bucket and key names.
     */
    public class DLStorageUriFactory : IDirectUriFactory {
        private readonly IStorageAdapter _adapter;

        /**
         * Initializes this DLStorageUriFactory.
         * <param name="adapter">The storage adapter</param>
         */
        public DLStorageUriFactory(IStorageAdapter adapter) {
            this._adapter = adapter;
        }

        /**
         * <see cref="IDirectUriFactory"/>
         */
        public override Uri BuildDirectUri(string bucket, string key) {
            var entityUri = new DLEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectUri(this._adapter.GetAccountName(),
                                         this._adapter.GetContainerName());
        }

        /**
         * <see cref="IDirectUriFactory"/>
         */
        public override Uri BuildDirectHttpsUri(string bucket, string key) {
            var entityUri = new DLEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectHttpsUri(this._adapter.GetAccountName(),
                                              this._adapter.GetContainerName());
        }
    }
}