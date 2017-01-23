using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChumBucket.Util {
    public interface IStorageAdapter {
        /**
         * Returns the account name.
         * <returns>The account name</returns>
         */ 
        string GetAccountName();

        /**
         * Returns the container name.
         * <returns>The container name</returns>
         */
        string GetContainerName();

        /**
         * Stores a file.
         * Must return a unique URI for subsequent access.
         * <param name="file">The file to store</param>
         * <param name="bucket">The bucket to store it in</param>
         * <returns>A unique URI for subsequent accesses</returns>
         */
        void Store(StorageFile file, string bucket);

        /**
         * Retrieves a file from a URI.
         * If the URI is invalid, must throw an ArgumentException.
         * If the file does not exist, must throw a KeyNotFoundException.
         * <param name="uri">The URI to retrieve</param>
         * <returns>The file</returns>
         */
        StorageFile Retrieve(EntityUri uri);

        /**
         * Deletes a file by a URI.
         * If the URI is invalid, must throw an ArgumentException.
         * If the file does not exist, must throw a KeyNotFoundException.
         * <param name="uri">The URI to delete</param>
         */
        void Delete(EntityUri uri);

        /**
         * Lists all buckets in this adapter.
         * <returns>An ICollection of bucket names</returns>
         */
        ICollection<EntityUri> ListBuckets();

        /**
         * Lists all StorageFile instances in this adapter.
         * <returns>An ICollection of StorageFiles</returns>
         */
        ICollection<EntityUri> ListFiles(string bucket);
    }
}