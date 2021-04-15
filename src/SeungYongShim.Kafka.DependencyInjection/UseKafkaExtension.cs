using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using SeungYongShim.Kafka.DependencyInjection;

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
