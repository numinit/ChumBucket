using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebRole.Util
{
    public class StorageFile
    {
        private Stream _stream;
        private string _name;
        private string _contentType;

        public Stream InputStream
        {
            get { return _stream; }
        }

        public string Name
        {
            get { return _name; }
        }

        public string ContentType
        {
            get { return _contentType; }
        }
        /**
         * Initializes this File.
         * <param name="stream">The stream to store</param>
         * <param name="name">A human-readable name for the file.</param>
         * <param name="contentType">The MIME type of the stored file.</param>
         */
        public StorageFile(Stream stream, string name, string contentType)
        {
            this._stream = stream;
            this._name = name;
            this._contentType = contentType;
        }
    }
    interface StorageAdapter
    {
        /**
         * Stores a file.
         * Must return a unique key for subsequent access.
         * <param name="file">The file to store</param>
         */
        string Store(StorageFile file);

        /**
         * Retrieves a file from a key.
         * If the key is invalid, must throw an ArgumentException.
         * If the file does not exist, must throw a KeyNotFoundException.
         * <param name="key">The key</param>
         */
        StorageFile Retrieve(string key);
    }
}
