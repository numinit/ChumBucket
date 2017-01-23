using ChumBucket.Util.Uris;
using System.IO;

namespace ChumBucket.Util.Storage {
    public class StorageFile {
        private readonly Stream _stream;
        private readonly EntityUri _uri;
        private readonly string _name;
        private readonly string _contentType;

        /**
         * Returns this file's input stream.
         */
        public Stream InputStream => this._stream;

        /**
         * Returns this file's EntityUri.
         */
        public EntityUri Uri => this._uri;

        /**
         * Returns this file's name.
         */
        public string Name => this._name;

        /**
         * Returns a suitable MIME type for this file.
         */
        public string ContentType {
            get {
                if (this._name != null && this._name.EndsWith(".csv")) {
                    return "text/csv";
                } else if (this._contentType != null) {
                    return this._contentType;
                } else {
                    return "application/octet-stream";
                }
            }
        }
  
        /**
         * Initializes this StorageFile.
         * <param name="stream">The stream to store</param>
         * <param name="uri">The file's URI</param>
         * <param name="name">The file's human-readable name</param>
         * <param name="contentType">The MIME type of the stored file.</param>
         */
        public StorageFile(Stream stream, EntityUri uri, string name, string contentType) {
            this._stream = stream;
            this._uri = uri;
            this._name = name;
            this._contentType = contentType;
        }

        /**
         * Initializes this StorageFile using the URI's key as the name.
         * <param name="stream">The stream to store</param>
         * <param name="uri">The file's URI</param>
         * <param name="contentType">The MIME type of the stored file.</param>
         */
        public StorageFile(Stream stream, EntityUri uri, string contentType) {
            this._stream = stream;
            this._uri = uri;
            this._name = uri.Key;
            this._contentType = contentType;
        }
    }
}