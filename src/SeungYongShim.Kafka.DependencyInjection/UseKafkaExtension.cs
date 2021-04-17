using System;
using Microsoft.Extensions.DependencyInjection;
using SeungYongShim.Kafka;

namespace Microsoft.Extensions.Hosting
{
    public static class UseKafkaExtension
    {
        public static IHostBuilder UseKafka(this IHostBuilder host,
                                            KafkaConfig kafkaConfig,
                                            params Type[] protobufMessageTypes)
        {
            host.ConfigureServices((host, services) =>
            {
                services.AddKafka(kafkaConfig, protobufMessageTypes);
            });

            return host;
        }
    }
}
