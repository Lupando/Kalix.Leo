using Kalix.Leo.Azure.Table;
using Lokad.Cloud.Storage.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using NUnit.Framework;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests.Table
{
    [TestFixture]
    public class AzureTableContextTests
    {
        protected CloudTable _table;
        protected AzureTableContext _azureTable;

        [SetUp]
        public virtual async Task Init()
        {
            var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudTableClient();
            _table = client.GetTableReference("kalixleotablecontext");
            await _table.CreateIfNotExistsAsync().ConfigureAwait(false);

            _azureTable = new AzureTableContext(_table, null);
        }

        [TearDown]
        public virtual async Task TearDown()
        {
            await _table.DeleteIfExistsAsync().ConfigureAwait(false);
        }

        [TestFixture]
        public class DeleteMethod : AzureTableContextTests
        {
            [Test]
            public async Task CanDeleteEvenWhenRowDoesNotExist()
            {
                _azureTable.Delete(new TestEntity { RowKey = "test1", PartitionKey = "delete" });
                _azureTable.Delete(new TestEntity { RowKey = "test2", PartitionKey = "delete" });

                _azureTable.Save().Wait();

                var query = new TableQuery<FatEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "delete"));
                var items = await _table.ExecuteQuerySegmentedAsync(query, null).ConfigureAwait(false);

                Assert.IsFalse(items.Any());
            }
        }
    }
}
