using ChumBucket.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;

namespace ChumBucket.Util {
    public class StorageFile {
        private Stream _stream;
        private EntityUri _uri;
        private string _name;
        private string _contentType;

        public Stream InputStream {
            get { return this._stream; }
        }

        public EntityUri Uri {
            get { return this._uri; }
        }

        public string Name {
            get { return this._name; }
        }

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