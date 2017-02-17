using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;

namespace WebRole.Util.DataLake {
    /**
     * A subclass of Stream that allows concatenation of multiple Stream
     * instances open for reading.
     */
    public class ConcatStream : Stream {
        private Queue<Stream> _streams;

        /**
         * Initializes this ConcatStream.
         * <param name="streams">The streams to concatenate</param>
         */
        public ConcatStream(params Stream[] streams) {
            this._streams = new Queue<Stream>(streams);
        }

        public override void Flush() {
            if (this._streams.Count > 0) {
                this._streams.Peek().Flush();
            }
        }

        public override long Seek(long offset, SeekOrigin origin) {
            throw new NotSupportedException();
        }

        public override void SetLength(long value) {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) {
            if (this._streams.Count > 0) {
                var stream = this._streams.Peek();
                var bytesRead = stream.Read(buffer, offset, count);
                if (bytesRead < count) {
                    // Partial read; dispose this stream
                    this._streams.Dequeue();
                    stream.Dispose();

                    // Read the next one
                    bytesRead += this.Read(buffer, offset + bytesRead, count - bytesRead);
                }

                return bytesRead;
            } else {
                return 0;
            }
        }

        public override void Write(byte[] buffer, int offset, int count) {
            throw new NotSupportedException();
        }

        public override bool CanRead => this._streams.Count > 0 && this._streams.Peek().CanRead;
        public override bool CanWrite => false;
        public override bool CanSeek => false;

        public override long Length {
            get { throw new NotSupportedException(); }
        }

        public override long Position {
            get { throw new NotSupportedException(); }
            set { throw new NotSupportedException(); }
        }
    }
}