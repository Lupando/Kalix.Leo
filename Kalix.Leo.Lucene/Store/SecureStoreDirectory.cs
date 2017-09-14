using Kalix.Leo.Core;
using Kalix.Leo.Encryption;
using Kalix.Leo.Storage;
using Lucene.Net.Store;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Path = System.IO.Path;

namespace Kalix.Leo.Lucene.Store
{
    public class SecureStoreDirectory : Directory
    {
        private readonly IMemoryCache _memoryCache;
        private readonly string _cachePrefix;
        private readonly ISecureStore _store;
        private readonly string _container;
        private readonly string _basePath;
        private readonly Lazy<Task<IEncryptor>> _encryptor;
        private readonly SecureStoreOptions _options;

        private LockFactory _lockFactory;

        public SecureStoreDirectory(ISecureStore store, string container, string basePath, Lazy<Task<IEncryptor>> encryptor, IMemoryCache memoryCache = null, string cachePrefix = null)
        {
            _container = container;
            _memoryCache = memoryCache ?? new MemoryCache(new MemoryCacheOptions());
            _cachePrefix = cachePrefix ?? "Lucene";
            _basePath = basePath ?? string.Empty;
            _store = store;
            _encryptor = encryptor ?? new Lazy<Task<IEncryptor>>(() => Task.FromResult((IEncryptor)null));

            _lockFactory = new SecureLockFactory(store, GetLocation);

            _options = SecureStoreOptions.None;
            if (_store.CanCompress)
            {
                _options = _options | SecureStoreOptions.Compress;
            }

            SafeTask.SafeWait(() => store.CreateContainerIfNotExists(container));
        }

        public override LockFactory LockFactory => _lockFactory;

        /// <summary>Returns an array of strings, one for each file in the directory. </summary>
        public override string[] ListAll()
        {
            return SafeTask.SafeResult(() => ListAllAsync());
        }

        private async Task<string[]> ListAllAsync()
        {
            int basePathLength = string.IsNullOrEmpty(_basePath) ? 0 : _basePath.Length + 1;

            return await _store
                .FindFiles(_container, string.IsNullOrEmpty(_basePath) ? null : (_basePath + '/'))
                .Select(s => s.Location.BasePath.Substring(basePathLength))
                .ToArray()
                .ConfigureAwait(false);
        }

        [Obsolete]
        public override bool FileExists(string name)
        {
            if(_memoryCache.TryGetValue<byte[]>(GetCacheKey(name), out var bytes))
            {
                return true;
            }

            var metadata = SafeTask.SafeResult(() => _store.GetMetadata(GetLocation(name)));
            return metadata != null;
        }

        /// <summary>Removes an existing file in the directory. </summary>
        public override void DeleteFile(string name)
        {
            var location = GetLocation(name);
            SafeTask.SafeWait(() => _store.Delete(location, null, _options));
            LeoTrace.WriteLine(String.Format("DELETE {0}", location.BasePath));

            _memoryCache.Remove(GetCacheKey(name));
        }

        /// <summary>Returns the length of a file in the directory. </summary>
        public override long FileLength(string name)
        {
            if (_memoryCache.TryGetValue<byte[]>(GetCacheKey(name), out var bytes))
            {
                return bytes.LongLength;
            }

            var metadata = SafeTask.SafeResult(() => _store.GetMetadata(GetLocation(name)));
            return metadata == null || !metadata.ContentLength.HasValue ? 0 : metadata.ContentLength.Value;
        }

        /// <summary>Creates a new, empty file in the directory with the given name.
        /// Returns a stream writing this file. 
        /// </summary>
        public override IndexOutput CreateOutput(string name, IOContext context)
        {
            var loc = GetLocation(name);

            var streamToken = new TaskCompletionSource<IWriteAsyncStream>();
            var completeToken = new TaskCompletionSource<bool>();

            var saveTask = Task.Run(() => CreateSaveTask(loc, streamToken, completeToken.Task));
            var stream = SafeTask.SafeResult(() => streamToken.Task);

            return new SecureStoreIndexOutput(name, stream, () =>
            {
                SafeTask.SafeWait(async () =>
                {
                    completeToken.SetResult(true);
                    var m = await saveTask.ConfigureAwait(false);

                    // We didn't know the content length until now, so save it as a final step
                    var metadata = new Metadata();
                    metadata.ContentLength = m.ContentLength.Value;

                    await _store.SaveMetadata(loc, metadata, _options).ConfigureAwait(false);
                });
            });
        }

        /// <summary>Returns a stream reading an existing file. </summary>
        public override IndexInput OpenInput(string name, IOContext context)
        {
            var data = SafeTask.SafeResult(() => _memoryCache.GetOrCreateAsync(GetCacheKey(name), async e =>
            {
                e.SetSlidingExpiration(TimeSpan.FromHours(1)).SetPriority(CacheItemPriority.Low);
                var loc = GetLocation(name);
                var enc = await _encryptor.Value.ConfigureAwait(false);
                var stream = await _store.LoadData(loc, null, enc).ConfigureAwait(false);
                if (stream == null) { throw new System.IO.FileNotFoundException(name); }

                return await stream.Stream.ReadBytes().ConfigureAwait(false);
            }));

            return new SecureStoreIndexInput(data);
        }

        public override void Sync(ICollection<string> names)
        {
            // Our index is pulling from blob storage first, so already 'synced'
        }

        public override void SetLockFactory(LockFactory lockFactory)
        {
            var current = _lockFactory as IDisposable;
            if (current != null)
            {
                current.Dispose();
            }
            _lockFactory = lockFactory;
        }

        public override Lock MakeLock(string name)
        {
            return _lockFactory.MakeLock(name);
        }

        public override void ClearLock(string name)
        {
            _lockFactory.ClearLock(name);
        }

        protected override void Dispose(bool disposing)
        {
            var lockFactory = _lockFactory as IDisposable;
            if (disposing && lockFactory != null)
            {
                lockFactory.Dispose();
            }
        }

        private string GetCacheKey(string name)
        {
            return $"{_cachePrefix}/{_container}/{_basePath}/{name}";
        }

        private StoreLocation GetLocation(string name)
        {
            return new StoreLocation(_container, Path.Combine(_basePath, name));
        }

        private async Task<Metadata> CreateSaveTask(StoreLocation loc, TaskCompletionSource<IWriteAsyncStream> streamToken, Task isComplete)
        {
            var encryptor = await _encryptor.Value.ConfigureAwait(false);
            return await _store.SaveData(loc, null, null, (s) =>
            {
                streamToken.SetResult(s);
                return isComplete;
            }, CancellationToken.None, encryptor, _options).ConfigureAwait(false);
        }
    }
}
