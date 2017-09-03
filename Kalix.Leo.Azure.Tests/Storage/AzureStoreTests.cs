using Kalix.Leo.Azure.Storage;
using Kalix.Leo.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Storage
{
    [TestFixture]
    public class AzureStoreTests
    {
        protected AzureStore _store;
        protected CloudBlockBlob _blob;
        protected StoreLocation _location;

        [SetUp]
        public virtual async Task Init()
        {
            _store = new AzureStore(CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient(), true);

            try
            {
                _blob = await AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata", true).ConfigureAwait(false);
                _location = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata");
            }
            catch (Exception e)
            {
                var ex = e;
            }
        }

        protected async Task<string> WriteData(StoreLocation location, Metadata m, byte[] data)
        {
            var ct = CancellationToken.None;
            var res = await _store.SaveData(location, m, null, async (s) =>
            {
                await s.WriteAsync(data, 0, data.Length, ct).ConfigureAwait(false);
                return data.Length;
            }, ct).ConfigureAwait(false);
            
            return res?.Snapshot;
        }

        protected Task<OptimisticStoreWriteResult> TryOptimisticWrite(StoreLocation location, Metadata m, byte[] data)
        {
            var ct = CancellationToken.None;
            return _store.TryOptimisticWrite(location, m, null, async (s) =>
            {
                await s.WriteAsync(data, 0, data.Length, ct).ConfigureAwait(false);
                return data.Length;
            }, ct);
        }

        [TestFixture]
        public class SaveDataMethod : AzureStoreTests
        {
            [Test]
            public async Task HasMetadataCorrectlySavesIt()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                await WriteData(_location, m, data).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
            }

            [Test]
            public async Task AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                await WriteData(_location, m, data).ConfigureAwait(false);

                var m2 = new Metadata();
                m2["metadata2"] = "othermetadata";
                await WriteData(_location, m2, data).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
            }

            [Test]
            public async Task WritesStoreVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.AreEqual("2.0", _blob.Metadata["leoazureversion"]);
            }

            [Test]
            public async Task MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                await WriteData(_location, null, data).ConfigureAwait(false);

                Assert.IsTrue(await _blob.ExistsAsync().ConfigureAwait(false));
            }
        }

        [TestFixture]
        public class TryOptimisticWriteMethod : AzureStoreTests
        {
            [Test]
            public async Task HasMetadataCorrectlySavesIt()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                var success = await TryOptimisticWrite(_location, m, data).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.IsTrue(success.Result);
                Assert.IsNotNull(success.Metadata.Snapshot);
                Assert.AreEqual("somemetadata", _blob.Metadata["metadata1"]);
            }

            [Test]
            public async Task AlwaysOverridesMetadata()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                var success1 = await TryOptimisticWrite(_location, m, data).ConfigureAwait(false);
                var oldMetadata = await _store.GetMetadata(_location).ConfigureAwait(false);

                var m2 = new Metadata();
                m2.ETag = oldMetadata.ETag;
                m2["metadata2"] = "othermetadata";
                var success2 = await TryOptimisticWrite(_location, m2, data).ConfigureAwait(false);
                var newMetadata = await _store.GetMetadata(_location).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.IsTrue(success1.Result, "first write failed");
                Assert.IsTrue(success2.Result, "second write failed");
                Assert.AreEqual(success1.Metadata.Snapshot, oldMetadata.Snapshot);
                Assert.AreEqual(success2.Metadata.Snapshot, newMetadata.Snapshot);
                Assert.AreNotEqual(success1.Metadata.Snapshot, success2.Metadata.Snapshot);
                Assert.IsFalse(_blob.Metadata.ContainsKey("metadata1"));
                Assert.AreEqual("othermetadata", _blob.Metadata["metadata2"]);
            }

            [Test]
            public async Task WritesStoreVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                await TryOptimisticWrite(_location, null, data).ConfigureAwait(false);

                await _blob.FetchAttributesAsync().ConfigureAwait(false);
                Assert.AreEqual("2.0", _blob.Metadata["leoazureversion"]);
            }

            [Test]
            public async Task NoETagMustBeNewSave()
            {
                var data = AzureTestsHelper.RandomData(1);
                var success1 = await TryOptimisticWrite(_location, null, data).ConfigureAwait(false);
                var success2 = await TryOptimisticWrite(_location, null, data).ConfigureAwait(false);

                Assert.IsTrue(success1.Result, "first write failed");
                Assert.IsFalse(success2.Result, "second write succeeded");
            }

            [Test]
            public async Task ETagDoesNotMatchFails()
            {
                var data = AzureTestsHelper.RandomData(1);
                var metadata = new Metadata { ETag = "\"0x8D49E94826A33D9\"" };
                var success = await TryOptimisticWrite(_location, metadata, data).ConfigureAwait(false);

                Assert.IsFalse(success.Result, "write should not have succeeded with fake eTag");
            }

            [Test]
            public async Task MultiUploadLargeFileIsSuccessful()
            {
                var data = AzureTestsHelper.RandomData(7);
                var success = await TryOptimisticWrite(_location, null, data).ConfigureAwait(false);

                Assert.IsTrue(success.Result);
                Assert.IsNotNull(success.Metadata.Snapshot);
                Assert.IsTrue(await _blob.ExistsAsync().ConfigureAwait(false));
            }
        }

        [TestFixture]
        public class GetMetadataMethod : AzureStoreTests
        {
            [Test]
            public async Task NoFileReturnsNull()
            {
                var result = await _store.GetMetadata(_location).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task FindsMetadataIncludingSizeAndLength()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "somemetadata";
                await WriteData(_location, m, data).ConfigureAwait(false);

                var result = await _store.GetMetadata(_location).ConfigureAwait(false);

                Assert.AreEqual("1048576", result[MetadataConstants.ContentLengthMetadataKey]);
                Assert.IsTrue(result.ContainsKey(MetadataConstants.ModifiedMetadataKey));
                Assert.IsNotNull(result.Snapshot);
                Assert.AreEqual("somemetadata", result["metadata1"]);
            }

            [Test]
            public async Task DoesNotReturnInternalVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                var result = await _store.GetMetadata(_location).ConfigureAwait(false);

                Assert.IsFalse(result.ContainsKey("leoazureversion"));
            }
        }

        [TestFixture]
        public class LoadDataMethod : AzureStoreTests
        {
            [Test]
            public async Task MetadataIsTransferedWhenSelectingAStream()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                await WriteData(_location, m, data).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                Assert.IsNotNull(result.Metadata.Snapshot);
                Assert.AreEqual("metadata", result.Metadata["metadata1"]);
            }

            [Test]
            public async Task NoFileReturnsFalse()
            {
                var result = await _store.LoadData(_location).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task NoContainerReturnsFalse()
            {
                var result = await _store.LoadData(new StoreLocation("blahblahblah", "blah")).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task FileMarkedAsDeletedReturnsNull()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["leodeleted"] = DateTime.UtcNow.Ticks.ToString();
                await WriteData(_location, m, data).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task AllDataLoadsCorrectly()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                byte[] resData;
                using(var ms = new MemoryStream())
                {
                    await result.Stream.CopyToStream(ms, CancellationToken.None).ConfigureAwait(false);
                    resData = ms.ToArray();
                }
                Assert.IsTrue(data.SequenceEqual(resData));
            }

            [Test]
            public async Task AllDataLargeFileLoadsCorrectly()
            {
                var data = AzureTestsHelper.RandomData(14);
                await WriteData(_location, null, data).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                byte[] resData;
                using (var ms = new MemoryStream())
                {
                    await result.Stream.CopyToStream(ms, CancellationToken.None).ConfigureAwait(false);
                    resData = ms.ToArray();
                }
                Assert.IsTrue(data.SequenceEqual(resData));
            }

            [Test]
            public async Task DoesNotReturnInternalVersion()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);

                Assert.IsFalse(result.Metadata.ContainsKey("leoazureversion"));
            }
        }

        [TestFixture]
        public class FindSnapshotsMethod : AzureStoreTests
        {
            [Test]
            public async Task NoSnapshotsReturnsEmpty()
            {
                var snapshots = await _store.FindSnapshots(_location).ToList().ConfigureAwait(false);

                Assert.AreEqual(0, snapshots.Count());
            }

            [Test]
            public async Task SingleSnapshotCanBeFound()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                await WriteData(_location, m, data).ConfigureAwait(false);

                var snapshots = await _store.FindSnapshots(_location).ToList().ConfigureAwait(false);

                Assert.AreEqual(1, snapshots.Count());
            }

            [Test]
            public async Task SubItemBlobSnapshotsAreNotIncluded()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                var blob2 = AzureTestsHelper.GetBlockBlob("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data", true);
                var location2 = new StoreLocation("kalix-leo-tests", "AzureStoreTests.testdata/subitem.data");

                await WriteData(location2, null, data).ConfigureAwait(false);

                var snapshots = await _store.FindSnapshots(_location).ToList().ConfigureAwait(false);
                
                Assert.AreEqual(1, snapshots.Count());
            }
        }

        [TestFixture]
        public class LoadDataMethodWithSnapshot : AzureStoreTests
        {
            [Test]
            public async Task MetadataIsTransferedWhenSelectingAStream()
            {
                var data = AzureTestsHelper.RandomData(1);
                var m = new Metadata();
                m["metadata1"] = "metadata";
                var shapshot = await WriteData(_location, m, data).ConfigureAwait(false);

                var res = await _store.LoadData(_location, shapshot).ConfigureAwait(false);
                Assert.AreEqual(shapshot, res.Metadata.Snapshot);
                Assert.AreEqual("metadata", res.Metadata["metadata1"]);
            }

            [Test]
            public async Task NoFileReturnsFalse()
            {
                var result = await _store.LoadData(_location, DateTime.UtcNow.Ticks.ToString()).ConfigureAwait(false);
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class SoftDeleteMethod : AzureStoreTests
        {
            [Test]
            public Task BlobThatDoesNotExistShouldNotThrowError()
            {
                return _store.SoftDelete(_location, null);
            }

            [Test]
            public async Task BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                await _store.SoftDelete(_location, null).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task ShouldNotDeleteSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);
                var shapshot = (await _store.FindSnapshots(_location).ToList().ConfigureAwait(false)).Single().Id;

                await _store.SoftDelete(_location, null).ConfigureAwait(false);

                var result = await _store.LoadData(_location, shapshot).ConfigureAwait(false);
                Assert.IsNotNull(result);
            }
        }

        [TestFixture]
        public class PermanentDeleteMethod : AzureStoreTests
        {
            [Test]
            public async Task BlobThatDoesNotExistShouldNotThrowError()
            {
                await _store.PermanentDelete(_location).ConfigureAwait(false);
            }

            [Test]
            public async Task BlobThatIsSoftDeletedShouldNotBeLoadable()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);

                await _store.PermanentDelete(_location).ConfigureAwait(false);

                var result = await _store.LoadData(_location).ConfigureAwait(false);
                Assert.IsNull(result);
            }

            [Test]
            public async Task ShouldDeleteAllSnapshots()
            {
                var data = AzureTestsHelper.RandomData(1);
                await WriteData(_location, null, data).ConfigureAwait(false);
                var shapshot = (await _store.FindSnapshots(_location).ToList().ConfigureAwait(false)).Single().Id;

                await _store.PermanentDelete(_location).ConfigureAwait(false);

                var result = await _store.LoadData(_location, shapshot).ConfigureAwait(false);
                Assert.IsNull(result);
            }
        }

        [TestFixture]
        public class LockMethod : AzureStoreTests
        {
            [Test]
            public async Task LockSuceedsEvenIfNoFile()
            {
                using(var l = await _store.Lock(_location).ConfigureAwait(false))
                {
                    Assert.IsNotNull(l);
                }
            }

            [Test]
            public async Task IfAlreadyLockedOtherLocksFail()
            {
                using(var l = await _store.Lock(_location).ConfigureAwait(false))
                using (var l2 = await _store.Lock(_location).ConfigureAwait(false))
                {
                    Assert.IsNotNull(l);
                    Assert.IsNull(l2);
                }
            }

            [Test]
            public void IfFileLockedReturnsFalse()
            {
                Assert.ThrowsAsync<LockException>(async () =>
                {
                    using (var l = await _store.Lock(_location).ConfigureAwait(false))
                    {
                        var data = AzureTestsHelper.RandomData(1);
                        await WriteData(_location, null, data).ConfigureAwait(false);
                    }
                });
            }
        }
    }
}
