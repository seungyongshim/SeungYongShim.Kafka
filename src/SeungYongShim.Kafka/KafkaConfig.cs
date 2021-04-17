using System;

namespace SeungYongShim.Kafka
{
    public record KafkaConfig(string Brokers, TimeSpan TimeOut);
}
