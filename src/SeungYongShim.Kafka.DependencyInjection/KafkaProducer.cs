using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public class KafkaProducer : IDisposable
    {
        private bool disposedValue;

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

                var message = JsonFormatter.ToDiagnosticString(m);

                var ret = await Producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = key,
                    Headers = new Headers { new Header("ClrType",
                                                           Encoding.UTF8.GetBytes(m.Descriptor.ClrType.ToString())),
                                                new Header("ActivityID",
                                                           Encoding.UTF8.GetBytes(Activity.Current?.Id ?? string.Empty))},
                    Value = message
                });

                activity?.AddTag("topic", topic);
                activity?.AddTag("message", message);

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

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    Producer?.Dispose();

                }

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
