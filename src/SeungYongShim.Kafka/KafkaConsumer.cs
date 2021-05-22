using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using SeungYongShim.ProtobufHelper;

namespace SeungYongShim.Kafka
{
    public class KafkaConsumer : IDisposable
    {
        private bool disposedValue;

        public KafkaConsumer(KafkaConfig kafkaConfig,
                             ProtoKnownTypes knownTypes,
                             ILogger<KafkaConsumer> logger)
        {
            Logger = logger;
            KafkaConfig = kafkaConfig;
            ProtoKnownTypes = knownTypes;
            ConsumeChannel = Channel.CreateBounded<(Headers, string, Action)>(10);
        }

        public ILogger<KafkaConsumer> Logger { get; }
        public KafkaConfig KafkaConfig { get; }
        public ProtoKnownTypes ProtoKnownTypes { get; }
        public Channel<(Headers, string, Action)> ConsumeChannel { get; }
        public CancellationTokenSource CancellationTokenSource { get; } = new CancellationTokenSource();
        public Thread KafkaConsumerThread { get; private set; }

        public async Task<Commitable> ConsumeAsync(TimeSpan timeOut)
        {
            var cts = new CancellationTokenSource(timeOut);
            var (headers, message, action) = await ConsumeChannel.Reader.ReadAsync(cts.Token);

            var activityId = headers.First(x => x.Key is "traceparent")?.GetValueBytes();
            var anyJson = JsonSerializer.Deserialize<AnyJson>(message);
            var o = ProtoKnownTypes.Unpack(anyJson.ToAny());

            using var activity = ActivitySourceStatic.Instance.StartActivity("kafka-consume", ActivityKind.Consumer, Encoding.Default.GetString(activityId));
            
            return new Commitable(o, action, activity.Id);
        }

        public void Start(string groupId,
                          IEnumerable<string> topics)
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

            KafkaConsumerThread = new Thread(async () =>
            {
                var token = CancellationTokenSource.Token;
                using (var consumer = new ConsumerBuilder<string, string>(config).Build())
                {
                    consumer.Subscribe(topics);

                    try
                    {
                        while (true)
                        {
                            token.ThrowIfCancellationRequested();
                            try
                            {
                                var cr = consumer.Consume(KafkaConfig.TimeOut);

                                if (cr is null) continue; // 이유를 현재 모르겠음
                                if (cr.IsPartitionEOF) continue;

                                await ConsumeChannel.Writer.WriteAsync((cr.Message.Headers,
                                                                        cr.Message.Value,
                                                                        () =>
                                                                        {
                                                                            try
                                                                            {
                                                                                consumer.Commit(cr);
                                                                            }
                                                                            catch (KafkaException e)
                                                                            {
                                                                                Logger.LogError($"Commit error: {e.Error.Reason}");
                                                                            }
                                                                        }
                                ));
                            }
                            catch (ConsumeException ex)
                            {
                                Logger.LogWarning(ex, "");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ConsumeChannel.Writer.TryComplete(ex);
                        Logger.LogError(ex, "");
                    }
                    finally
                    {
                        consumer.Close();
                        ConsumeChannel.Writer.TryComplete();
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
