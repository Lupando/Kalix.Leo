using Lucene.Net.Store;
using System;
using System.Threading;
using Kalix.Leo.Core;

namespace Kalix.Leo.Lucene.Store
{
    /// <summary>
    /// Implements IndexOutput semantics for a write/append only file
    /// </summary>
    public class SecureStoreIndexOutput : BufferedIndexOutput
    {
        private readonly IWriteAsyncStream _writeStream;
        private readonly Action _onComplete;
        private long _length;
        private bool _hasDisposed;

        public SecureStoreIndexOutput(string path, IWriteAsyncStream writeStream, Action onComplete)
        {
            _writeStream = writeStream;
            _onComplete = onComplete;
            _length = 0;
        }

        public override long Length => _length;

        protected override void FlushBuffer(byte[] b, int offset, int len)
        {
            if (_hasDisposed) { throw new ObjectDisposedException(nameof(SecureStoreIndexOutput)); }

            _length += len;
            SafeTask.SafeWait(() => _writeStream.WriteAsync(b, offset, len, CancellationToken.None));
        }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (!_hasDisposed)
            {
                _hasDisposed = true;
                _onComplete();
            }
        }
    }
}
