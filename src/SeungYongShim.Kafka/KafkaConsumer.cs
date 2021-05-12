using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using Confluent.Kafka;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using SeungYongShim.ProtobufHelper;

namespace SeungYongShim.Kafka
{
    public class KafkaConsumer : IDisposable
    {
        private bool disposedValue;

        public KafkaConsumer(ActivitySource activitySource,
                             KafkaConfig kafkaConfig,
                             ProtoKnownTypes knownTypes,
                             ILogger<KafkaConsumer> logger)
        {
            ActivitySource = activitySource;
            Logger = logger;
            KafkaConfig = kafkaConfig;
            ProtoKnownTypes = knownTypes;
        }

        public ActivitySource ActivitySource { get; }
        public ILogger<KafkaConsumer> Logger { get; }
        public KafkaConfig KafkaConfig { get; }
        public ProtoKnownTypes ProtoKnownTypes { get; }

        public CancellationTokenSource CancellationTokenSource { get; } = new CancellationTokenSource();
        public Thread KafkaConsumerThread { get; private set; }

        public void Run(string groupId,
                        IEnumerable<string> topics,
                        Action<Commitable> callback)
        {
            if (KafkaConsumerThread is not null) return;

            var cancellationToken = CancellationTokenSource.Token;
            var timeout = TimeSpan.FromMinutes(10);

            var config = new ConsumerConfig
            {
                BootstrapServers = KafkaConfig.Brokers,
                GroupId = groupId,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };

            KafkaConsumerThread = new Thread(() =>
            {
                var slim = new ManualResetEventSlim();

                using (var consumer = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumer.Subscribe(topics);

                    try
                    {
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(KafkaConfig.TimeOut);

                                if (consumeResult is null) continue; // 이유를 현재 모르겠음

                                if (consumeResult.IsPartitionEOF) continue;

                                var anyJson = JsonSerializer.Deserialize<AnyJson>(consumeResult.Message.Value);
                                var o = ProtoKnownTypes.Unpack(anyJson.ToAny());

                                var activityID = consumeResult.Message.Headers.First(x => x.Key is "ActivityID").GetValueBytes();
                                
                                Action action = () =>
                                {
                                    try
                                    {
                                        consumer.Commit(consumeResult);
                                    }
                                    catch (KafkaException e)
                                    {
                                        Logger.LogError($"Commit error: {e.Error.Reason}");
                                    }
                                    finally
                                    {
                                        slim.Set();
                                    }
                                };

                                var message = new Commitable(o, consumeResult.Message.Key, action);

                                using (var activity = ActivitySource?.StartActivity("kafka consume", ActivityKind.Consumer, Encoding.Default.GetString(activityID)))
                                {
                                    callback?.Invoke(message);
                                }

                                slim.Wait(timeout, cancellationToken);
                                slim.Reset();
                            }
                            catch (ConsumeException e)
                            {
                                Logger.LogError(e, "Consume Error");
                            }
                            catch (TimeoutException)
                            {
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.LogDebug("Closing consumer.");
                    }
                    finally
                    {
                        consumer.Close();
                        KafkaConsumerThread = null;
                    }
                }
            });

            KafkaConsumerThread.Start();
        }

        public void Stop() => CancellationTokenSource.Cancel();

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Stop();
                    KafkaConsumerThread.Join(TimeSpan.FromSeconds(5));
                }

                KafkaConsumerThread = null;
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
