using ChumBucket.Util.Uris;
using System.Collections.Generic;

namespace ChumBucket.Util.Storage {
    /**
     * An adapter for file storage in different locations.
     * At minimum, a storage adapter must have an account name
     * and a container name.
     */

    public abstract class IStorageAdapter {
        public abstract string GetAccountName();
        public abstract string GetContainerName();
        public abstract void Store(StorageFile file, string bucket);
        public abstract StorageFile Retrieve(EntityUri uri);
        public abstract void Delete(EntityUri uri);
        public abstract ICollection<EntityUri> ListBuckets();
        public abstract ICollection<EntityUri> ListFiles(string bucket);
    }
}