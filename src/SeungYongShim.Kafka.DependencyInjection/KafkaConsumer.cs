using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SeungYongShim.Kafka.DependencyInjection.Abstractions;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public class KafkaConsumer : IDisposable
    {
        private bool disposedValue;

        public KafkaConsumer(ActivitySource activitySource, KafkaConfig kafkaConfig, KafkaProtobufMessageTypes kafkaConsumerMessageTypes, ILogger<KafkaConsumer> logger)
        {
            ActivitySource = activitySource;
            Logger = logger;
            KafkaConfig = kafkaConfig;
            KafkaConsumerMessageTypes = kafkaConsumerMessageTypes;
        }

        public ActivitySource ActivitySource { get; }
        public ILogger<KafkaConsumer> Logger { get; }
        public KafkaConfig KafkaConfig { get; }
        public KafkaProtobufMessageTypes KafkaConsumerMessageTypes { get; }

        public CancellationTokenSource CancellationTokenSource { get; } = new CancellationTokenSource();
        public Thread KafkaConsumerThread { get; private set; }

        public void Run(string groupId,
                        IList<string> topics,
                        Action<ICommitable> callback)
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

                                if (consumeResult.IsPartitionEOF) continue;

                                var clrType = consumeResult.Message.Headers.First(x => x.Key is "ClrType").GetValueBytes();
                                var typeName = Encoding.Default.GetString(clrType);
                                var messageType = KafkaConsumerMessageTypes.GetTypeAll[typeName];
                                var parser = KafkaConsumerMessageTypes.GetParserAll[typeName];
                                var o = parser.ParseJson(consumeResult.Message.Value);
                                var type = typeof(Commitable<>).MakeGenericType(messageType);

                                var activityID = consumeResult.Message.Headers.First(x => x.Key is "ActivityID").GetValueBytes();
                                using var activity = ActivitySource?.StartActivity("KafkaConsumer", ActivityKind.Consumer, Encoding.Default.GetString(activityID));

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

                                var message = Activator.CreateInstance(type, o, consumeResult.Message.Key, action);

                                callback?.Invoke(message as ICommitable);

                                slim.Wait(timeout, cancellationToken);
                                slim.Reset();
                            }
                            catch (ConsumeException e)
                            {
                                Logger.LogError(e, "Consume Error");
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
