using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Tests
{
    public static class AzureTestsHelper
    {
        private const long KB = 1024;
        private const long MB = 1024 * KB;

        private static Dictionary<string, CloudBlobContainer> _containers = new Dictionary<string,CloudBlobContainer>();
        private static Random _random = new Random();

        public static async Task<CloudBlobContainer> GetContainer(string name)
        {
            if(!_containers.ContainsKey(name))
            {
                //CloudStorageAccount.Parse("UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://ipv4.fiddler")
                var client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudBlobClient();
                var container = client.GetContainerReference(name);
                await container.CreateIfNotExistsAsync().ConfigureAwait(false);

                _containers[name] = container;
            }

            return _containers[name];
        }

        public static async Task<CloudBlockBlob> GetBlockBlob(string container, string path, bool del)
        {
            var c = await GetContainer(container).ConfigureAwait(false);
            var b = c.GetBlockBlobReference(path);
            if (del)
            {
                await b.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, null, null).ConfigureAwait(false);
            }

            return b;
        }

        public static byte[] RandomData(long noOfMb)
        {
            var data = new byte[noOfMb * MB];
            _random.NextBytes(data);
            return data;
        }
    }
}
