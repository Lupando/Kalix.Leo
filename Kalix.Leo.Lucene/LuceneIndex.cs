using Kalix.Leo.Encryption;
using Kalix.Leo.Lucene.Analysis;
using Kalix.Leo.Lucene.Store;
using Kalix.Leo.Storage;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Miscellaneous;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Lucene
{
    public class LuceneIndex : IDisposable, ILuceneIndex
    {
        private readonly Directory _directory;
        private readonly Analyzer _analyzer;

        private Lazy<SearcherManager> _reader;
        private DateTime _lastRead;

        private SearcherContextInternal _writer;
        private object _writerLock = new object();
        private bool _isDisposed;

        /// <summary>
        /// Create a lucene index over the top of a secure store, using an encrypted file cache and english analyzer
        /// Only one instance should be used for both indexing and searching (on any number of threads) for best results
        /// </summary>
        /// <param name="store">Store to have the Indexer on top of</param>
        /// <param name="container">Container to put the index</param>
        /// <param name="basePath">The path to namespace this index in</param>
        /// <param name="encryptor">The encryptor to encryt any records being saved</param>
        /// <param name="cache">Use the specified memory cache to store files in memory</param>
        /// <param name="cachePrefix">Caching namespace for memory files</param>
        public LuceneIndex(ISecureStore store, string container, string basePath, Lazy<Task<IEncryptor>> encryptor, IMemoryCache cache = null, string cachePrefix = null)
        {
            encryptor = encryptor ?? new Lazy<Task<IEncryptor>>(() => Task.FromResult((IEncryptor)null));
            
            _directory = new SecureStoreDirectory(store, container, basePath, encryptor, cache, cachePrefix);
            _analyzer = new EnglishAnalyzer();

            _reader = new Lazy<SearcherManager>(() => BuildSearcherManagerReader(_directory, _analyzer), true);
        }

        /// <summary>
        /// Lower level constructor, put in your own cache, (lucene) directory and (lucene) analyzer
        /// </summary>
        /// <param name="directory">Lucene directory of your files</param>
        /// <param name="analyzer">Analyzer you want to use for your indexing/searching</param>
        public LuceneIndex(Directory directory, Analyzer analyzer)
        {
            _directory = directory;
            _analyzer = analyzer;

            _reader = new Lazy<SearcherManager>(() => BuildSearcherManagerReader(_directory, _analyzer), true);
        }

        public Task WriteToIndex(IEnumerable<Document> documents, bool waitForGeneration = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = GetWriter();
            long gen = 0;
            foreach(var d in documents)
            {
                gen = writer.AddDocument(d);
            }

            if(waitForGeneration)
            {
                // This is a bit hacky but no point waiting for the generation but then
                // not commiting it so that it can be used on other machines
                _writer.ForceCommit();
            }
            return Task.FromResult(0);
        }

        public async Task WriteToIndex(Func<TrackingIndexWriter, Task> writeUsingIndex, bool waitForGeneration = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            var writer = GetWriter();
            await writeUsingIndex(writer).ConfigureAwait(false);

            if (waitForGeneration)
            {
                // This is a bit hacky but no point waiting for the generation but then
                // not commiting it so that it can be used on other machines
                _writer.ForceCommit();
            }
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, TopDocs> doSearchFunc, bool forceCheck = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            return SearchDocuments((s, a) => doSearchFunc(s), forceCheck);
        }

        public IEnumerable<Document> SearchDocuments(Func<IndexSearcher, Analyzer, TopDocs> doSearchFunc, bool forceCheck = false)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }
            
            var manager = GetSearcherManager(forceCheck);
            var reader = manager.Acquire();

            try
            {
                var docs = doSearchFunc(reader, _analyzer);

                foreach (var doc in docs.ScoreDocs)
                {
                    yield return reader.Doc(doc.Doc);
                }
            }
            finally
            {
                manager.Release(reader);
            }
        }

        public Task DeleteAll()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("LuceneIndex");
            }

            if (DirectoryReader.IndexExists(_directory))
            {
                var writer = GetWriter();
                writer.DeleteAll();
            }

            return Task.FromResult(0);
        }

        private SearcherManager GetSearcherManager(bool forceCheck)
        {
            var searcher = _writer?.GetSearcher() ?? _reader.Value;
            
            if (forceCheck || _lastRead.AddSeconds(5) < DateTime.UtcNow)
            {
                searcher.MaybeRefreshBlocking();
                _lastRead = DateTime.UtcNow;
            }

            return searcher;
        }

        private TrackingIndexWriter GetWriter()
        {
            if(_writer == null)
            {
                lock (_writerLock)
                {
                    if (_writer == null)
                    {
                        _writer = new SearcherContextInternal(_directory, _analyzer);
                    }
                }
            }

            // Once we have the writer that takes over!
            if(_reader != null)
            {
                // Note: we are specifically not disposing here so that any queries can finish on the old reader
                _reader = null;
            }

            return _writer.Writer;
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                if(_reader != null && _reader.IsValueCreated)
                {
                    _reader.Value.Dispose();
                }
                if(_writer != null)
                {
                    _writer.Dispose();
                }
                _analyzer.Dispose();
                _directory.Dispose();
            }
        }
        
        private static SearcherManager BuildSearcherManagerReader(Directory directory, Analyzer analyzer)
        {
            if (!DirectoryReader.IndexExists(directory))
            {
                // this index doesn't exist... make it!
                using (new IndexWriter(directory, new IndexWriterConfig(LeoLuceneVersion.Version, analyzer)
                {
                    OpenMode = OpenMode.CREATE_OR_APPEND
                })) { }
            }

            return new SearcherManager(directory, null);
        }

        private class SearcherContextInternal : IDisposable
        {
            public PerFieldAnalyzerWrapper Analyzer { get; private set; }

            private readonly IndexWriter _writer;
            private readonly TrackingIndexWriter _trackingWriter;
            private readonly SearcherManager _searcherManager;
            private readonly ControlledRealTimeReopenThread<IndexSearcher> _nrtReopenThread;

            public SearcherContextInternal(Directory dir, Analyzer defaultAnalyzer)
                : this(dir, defaultAnalyzer, TimeSpan.FromSeconds(.1), TimeSpan.FromSeconds(2))
            {
            }

            public SearcherContextInternal(Directory dir, Analyzer defaultAnalyzer, TimeSpan targetMinStale, TimeSpan targetMaxStale)
            {
                Analyzer = new PerFieldAnalyzerWrapper(defaultAnalyzer);
                _writer = new IndexWriter(dir, new IndexWriterConfig(LeoLuceneVersion.Version, Analyzer)
                {
                    OpenMode = OpenMode.CREATE_OR_APPEND
                });
                _trackingWriter = new TrackingIndexWriter(_writer);
                _searcherManager = new SearcherManager(_writer, true, null);
                _nrtReopenThread = new ControlledRealTimeReopenThread<IndexSearcher>(_trackingWriter, _searcherManager, targetMaxStale.TotalSeconds, targetMinStale.TotalSeconds);
                _nrtReopenThread.SetDaemon(true);
                _nrtReopenThread.Start();
            }

            public TrackingIndexWriter Writer => _trackingWriter;

            public SearcherManager GetSearcher()
            {
                return _searcherManager;
            }

            public void ForceCommit()
            {
                _writer.WaitForMerges();
                _writer.Commit();
            }

            public void Dispose()
            {
                _nrtReopenThread.Dispose();
                _searcherManager.Dispose();
                _writer.Dispose();
            }
        }
    }
}
