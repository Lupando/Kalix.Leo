﻿using Kalix.Leo.Queue;
using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Kalix.Leo.Azure.Queue
{
    public class AzureQueueStorage : IQueue
    {
        private readonly CloudQueue _queue;

        public AzureQueueStorage(CloudQueueClient queueClient, string queue)
        {
            _queue = queueClient.GetQueueReference(queue);
        }

        public Task SendMessage(string data)
        {
            return _queue.AddMessageAsync(new CloudQueueMessage(data));
        }

        public IObservable<IQueueMessage> ListenForMessages(Action<Exception> uncaughtException = null, int? messagesToProcessInParallel = null, int millisecondPollInterval = 2000)
        {
            return Observable.Create<IQueueMessage>(observer =>
            {
                var prefetchCount = messagesToProcessInParallel ?? Environment.ProcessorCount;
                var cancel = new CancellationDisposable();

                Task.Run(async () =>
                {
                    int counter = 0;
                    object counterLock = new object();
                    while(!cancel.Token.IsCancellationRequested)
                    {
                        bool doDelay = true;
                        try
                        {
                            if (counter <= prefetchCount)
                            {
                                LeoTrace.WriteLine("Getting new messages in queue");
                                var messages = await _queue.GetMessagesAsync(prefetchCount, TimeSpan.FromMinutes(1), null, null);

                                lock (counterLock)
                                {
                                    counter += messages.Count();
                                }

                                foreach (var m in messages)
                                {
                                    var message = new AzureQueueStorageMessage(_queue, m, () => { Interlocked.Decrement(ref counter); });
                                    observer.OnNext(message);
                                    doDelay = false;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (uncaughtException != null)
                            {
                                uncaughtException(e);
                            }
                            counter = 0;
                            doDelay = true;
                        }

                        if (doDelay)
                        {
                            LeoTrace.WriteLine("Waiting to poll in queue");
                            await Task.Delay(millisecondPollInterval);
                        }
                    }
                }, cancel.Token);

                // Return the item to dispose when done
                return cancel;

            });
        }

        public Task CreateQueueIfNotExists()
        {
            return _queue.CreateIfNotExistsAsync();
        }

        public Task DeleteQueueIfExists()
        {
            return _queue.DeleteIfExistsAsync();
        }
    }
}
