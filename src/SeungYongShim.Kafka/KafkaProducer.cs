using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Context.Propagation;

namespace SeungYongShim.Kafka
{
    public class KafkaProducer : IDisposable
    {
        private bool disposedValue;
        private static readonly TextMapPropagator Propagator = Propagators.DefaultTextMapPropagator;

        public KafkaProducer(KafkaConfig kafkaConfig, ILogger<KafkaConsumer> logger)
        {
            KafkaConfig = kafkaConfig;
            _log = logger;

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
                using var activity = ActivitySourceStatic.Instance.StartActivity("kafka", ActivityKind.Producer);

                var message = JsonFormatter.ToDiagnosticString(Any.Pack(m));
                var headers = new Headers()
                {
                    new Header("traceparent", Encoding.UTF8.GetBytes(activity?.Id ?? string.Empty))
                };

                var ret = await Producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = key,
                    Headers = headers,
                    Value = message
                }); ;

                activity?.AddTag("topic", topic);
                activity?.AddTag("message", message);
            }
            catch (Exception ex)
            {
                _log.LogWarning(ex, "");
                throw;
            }
        }

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
