using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChumBucket.Util {
    public class StorageFile {
        private Stream _stream;
        private string _name;
        private string _contentType;

        public Stream InputStream {
            get { return _stream; }
        }

        public string Name {
            get { return _name; }
        }

        public string ContentType {
            get { return _contentType; }
        }
        /**
         * Initializes this File.
         * <param name="stream">The stream to store</param>
         * <param name="name">A human-readable name for the file.</param>
         * <param name="contentType">The MIME type of the stored file.</param>
         */
        public StorageFile(Stream stream, string name, string contentType) {
            this._stream = stream;
            this._name = name;
            this._contentType = contentType;
        }
    }

    public abstract class UriKeyable {
        /**
         * Throws an ArgumentException if this UriKeyable
         * does not accept the specified URI.
         * <param name="uri">The URI</param>
         */
        public void AssertAccepts(Uri uri) {
            if (!this.WillAccept(uri)) {
                throw new ArgumentException("illegal URI");
            }
        }

        /**
         * Returns whether this UriKeyable will accept the specified URI.
         * <param name="uri">The URI</param>
         * <returns>True if we can handle this URI, false otherwise</returns>
         */
        public abstract bool WillAccept(Uri uri);

        /**
         * Builds a URI for this UriKeyable from a GUID.
         * <param name="guid">The GUID</param>
         */
        public abstract Uri BuildUri(Guid guid);
    }

    public abstract class StorageAdapter : UriKeyable {
        /**
         * Stores a file.
         * Must return a unique URI for subsequent access.
         * <param name="file">The file to store</param>
         * <returns>A unique URI for subsequent accesses</returns>
         */
        public Uri Store(StorageFile file) {
            return this.Store(file, Guid.NewGuid());
        }
  
        /**
         * Stores a file.
         * Must return a unique URI for subsequent access.
         * <param name="file">The file to store</param>
         * <param name="guid">The GUID to key the stored file</param>
         * <returns>A unique URI for subsequent accesses</returns>
         */
        public abstract Uri Store(StorageFile file, Guid guid);

        /**
         * Retrieves a file from a URI.
         * If the URI is invalid, must throw an ArgumentException.
         * If the file does not exist, must throw a KeyNotFoundException.
         * <param name="uri">The URI to retrieve</param>
         * <returns>The file</returns>
         */
        public abstract StorageFile Retrieve(Uri uri);

        /**
         * Lists all StorageFile instances in this adapter.
         * <returns>An ICollection of StorageFiles</returns>
         */
        public abstract ICollection<StorageFile> List();
    }
}
