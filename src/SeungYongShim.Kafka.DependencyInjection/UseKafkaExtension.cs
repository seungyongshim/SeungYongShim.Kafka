using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using SeungYongShim.Kafka;


namespace Microsoft.Extensions.Hosting
{
    public static class UseKafkaExtension
    {
        public static IHostBuilder UseKafka(this IHostBuilder host,
                                            KafkaConfig kafkaConfig,
                                            params string[] searchPatterns) 
        {
            host.ConfigureServices((host, services) =>
            {
                services.AddKafka(kafkaConfig, searchPatterns.Append("SeungYongShim.Kafka*.dll"));
            });

            return host;
        }
    }
}
