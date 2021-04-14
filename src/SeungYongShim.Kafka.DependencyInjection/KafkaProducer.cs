using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public class KafkaProducer
    {
        public KafkaProducer(ActivitySource activitySource, KafkaConfig kafkaConfig, ILogger<KafkaConsumer> logger)
        {
            ActivitySource = activitySource;
            _log = logger;
            KafkaConfig = kafkaConfig;

            var config = new ProducerConfig
            {
                BootstrapServers = kafkaConfig.Brokers,
                Acks = Acks.All,
            };

            Producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task SendAsync(IMessage m, string topic, string key = "1")
            {
                try
                {
                    using var activity = ActivitySource.StartActivity("kafka", ActivityKind.Producer);
                    activity?.AddTag("topic", topic);

                    var ret = await Producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = key,
                        Headers = new Headers { new Header("ClrType",
                                                           Encoding.UTF8.GetBytes(m.Descriptor.ClrType.ToString())),
                                                new Header("ActivityID",
                                                           Encoding.UTF8.GetBytes(Activity.Current?.Id ?? string.Empty))},
                        Value = JsonFormatter.ToDiagnosticString(m)
                    });
    }
                catch (Exception ex)
                {
                    _log.LogWarning(ex, "");
                }
            }

public ActivitySource ActivitySource { get; }

        private ILogger<KafkaConsumer> _log { get; }

        public KafkaConfig KafkaConfig { get; }

        private IProducer<string, string> Producer { get; }
    }
}
