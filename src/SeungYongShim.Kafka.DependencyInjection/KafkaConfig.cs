using System;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public record KafkaConfig(string Brokers, TimeSpan TimeOut);
}
