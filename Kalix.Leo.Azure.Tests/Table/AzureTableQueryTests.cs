using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using NUnit.Framework;
using System.Text;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Table
{
    [TestFixture]
    public class AzureTableQueryTests
    {
        protected CloudTable _table;
        protected AzureTableQuery<TestEntity> _query;

        [SetUp]
        public virtual async Task Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudTableClient();
            _table = client.GetTableReference("kalixleotablequery");
            await _table.CreateIfNotExistsAsync().ConfigureAwait(false);

            await _table.ExecuteAsync(TableOperation.InsertOrReplace(BuildEntity("test1", "test1"))).ConfigureAwait(false);
            await _table.ExecuteAsync(TableOperation.InsertOrReplace(BuildEntity("test1", "test2"))).ConfigureAwait(false);
            await _table.ExecuteAsync(TableOperation.InsertOrReplace(BuildEntity("test2", "test1"))).ConfigureAwait(false);
            await _table.ExecuteAsync(TableOperation.InsertOrReplace(BuildEntity("test2", "test2"))).ConfigureAwait(false);

            _query = new AzureTableQuery<TestEntity>(_table, null);
        }

        [TearDown]
        public virtual async Task TearDown()
        {
            await _table.DeleteIfExistsAsync().ConfigureAwait(false);
        }

        [TestFixture]
        public class CountMethod : AzureTableQueryTests
        {
            [Test]
            public void CountsCorrectly()
            {
                var count = _query.Count().Result;
                Assert.AreEqual(4, count);
            }

            [Test]
            public void CountWithFilterCorrectly()
            {
                var count = _query.PartitionKeyEquals("test1").Count().Result;
                Assert.AreEqual(2, count);
            }
        }

        private FatEntity BuildEntity(string partitionKey, string rowKey)
        {
            var e = new FatEntity { PartitionKey = partitionKey, RowKey = rowKey, ETag = "*" };
            var data = new { testdata = "blah" };
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data));
            e.SetData(bytes, bytes.Length);
            return e;
        }
    }
}
