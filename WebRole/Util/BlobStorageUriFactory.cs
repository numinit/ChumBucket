using ChumBucket.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebRole.Util {
    public class BlobStorageUriFactory : IDirectUriFactory {
        private IStorageAdapter _adapter;
        public BlobStorageUriFactory(IStorageAdapter adapter) {
            this._adapter = adapter;
        }

        public Uri BuildDirectUri(string bucket, string key) {
            var entityUri = new BlobStorageEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectUri(this._adapter.GetAccountName(),
                                         this._adapter.GetContainerName());
        }

        public Uri BuildDirectHttpsUri(string bucket, string key) {
            var entityUri = new BlobStorageEntityUri(bucket: bucket, key: key);
            return entityUri.ToDirectHttpsUri(this._adapter.GetAccountName(),
                                              this._adapter.GetContainerName());
        }
    }
}