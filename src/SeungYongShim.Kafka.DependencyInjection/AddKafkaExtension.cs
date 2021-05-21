using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using SeungYongShim.Kafka;
using SeungYongShim.ProtobufHelper;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class AddKafkaExtension
    {
        internal static IServiceCollection AddKafka(this IServiceCollection services,
                                                    KafkaConfig kafkaConfig,
                                                    IEnumerable<string> searchPatterns)
        {
            services.AddTransient<KafkaConsumer>();
            services.AddTransient<KafkaProducer>();
            services.AddSingleton(sp => new ProtoKnownTypes(searchPatterns.ToArray()));
            services.AddSingleton(sp => kafkaConfig);

            return services;
        }
    }
}
