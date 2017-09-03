using Kalix.Leo.Storage;
using Lucene.Net.Store;
using System;
using System.Collections.Generic;

namespace Kalix.Leo.Lucene.Store
{
    public sealed class SecureLockFactory : LockFactory, IDisposable
    {
        private readonly ISecureStore _store;
        private readonly Func<string, StoreLocation> _locationTranslate;
        private readonly Dictionary<string, SecureStoreLock> _locks = new Dictionary<string, SecureStoreLock>();

        public SecureLockFactory(ISecureStore store, Func<string, StoreLocation> locationTranslate)
        {
            _store = store;
            _locationTranslate = locationTranslate;
        }

        public override void ClearLock(string lockName)
        {
            lock (_locks)
            {
                if (_locks.ContainsKey(lockName))
                {
                    var lk = _locks[lockName];
                    _locks.Remove(lockName);
                    lk.Dispose();
                }
            }
        }

        public override Lock MakeLock(string lockName)
        {
            lock (_locks)
            {
                if (!_locks.ContainsKey(lockName))
                {
                    _locks.Add(lockName, new SecureStoreLock(_store, _locationTranslate(lockName)));
                }

                return _locks[lockName];
            }
        }

        public void Dispose()
        {
            foreach (var l in _locks.Values)
            {
                l.Dispose();
            }
        }
    }
}
