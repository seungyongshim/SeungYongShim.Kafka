using System;
using System.Diagnostics;
using SeungYongShim.Kafka;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class AddKafkaExtension
    {
        internal static IServiceCollection AddKafka(this IServiceCollection services,
                                                    KafkaConfig kafkaConfig,
                                                    params Type[] protobufMessageTypes)
        {
            services.AddSingleton(new ActivitySource("SeungYongShim.Kafka.DependencyInjection"));
            services.AddTransient<KafkaConsumer>();
            services.AddTransient<KafkaProducer>();
            services.AddSingleton(sp => new KafkaProtobufMessageTypes(protobufMessageTypes));
            services.AddSingleton(sp => kafkaConfig);

            return services;
        }
    }
}
