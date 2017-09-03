using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kalix.Leo.Amazon.Tests
{
    public static class AmazonTestsHelper
    {
        private const long KB = 1024;

        private static List<string> _containers = new List<string>();
        private static AmazonS3Client _client;
        private static Random _random = new Random();

        static AmazonTestsHelper()
        {
            var region = RegionEndpoint.GetBySystemName("ap-southeast-2");
            _client = new AmazonS3Client(region);
        }

        public static async Task<AmazonS3Client> SetupBlob(string container, string path)
        {
            var request = new ListVersionsRequest
            {
                BucketName = container,
                Prefix = path
            };

            var resp = await _client.ListVersionsAsync(request).ConfigureAwait(false);
            var toDelete = resp.Versions.Where(v => v.Key == path).Select(v => new KeyVersion
            {
                Key = v.Key,
                VersionId = v.VersionId
            }).ToList();

            if (toDelete.Any())
            {
                var delRequest = new DeleteObjectsRequest
                {
                    BucketName = container,
                    Objects = toDelete,
                    Quiet = true
                };

                await _client.DeleteObjectsAsync(delRequest).ConfigureAwait(false);
            }

            return _client;
        }

        public static byte[] RandomData(long noOfKb)
        {
            var data = new byte[noOfKb * KB];
            _random.NextBytes(data);
            return data;
        }
    }
}
