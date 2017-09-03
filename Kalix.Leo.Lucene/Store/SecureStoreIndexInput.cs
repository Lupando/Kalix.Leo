using Lucene.Net.Store;
using System.IO;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreIndexInput : IndexInput
    {
        private readonly MemoryStream _stream;

        public SecureStoreIndexInput(byte[] data) 
            : base("SecureStoreIndex")
        {
            _stream = new MemoryStream(data, 0, data.Length, false, true);
        }

        public override long Length => _stream.Length;

        public override long GetFilePointer()
        {
            return _stream.Position;
        }

        public override byte ReadByte()
        {
            return (byte)_stream.ReadByte();
        }

        public override void ReadBytes(byte[] b, int offset, int len)
        {
            _stream.Read(b, offset, len);
        }

        public override void Seek(long pos)
        {
            _stream.Seek(pos, SeekOrigin.Begin);
        }

        public override object Clone()
        {
            return new SecureStoreIndexInput(_stream.GetBuffer());
        }

        protected override void Dispose(bool disposing)
        {
            _stream.Dispose();
        }
    }
}
