using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public record KafkaConfig(string Brokers, TimeSpan TimeOut)
    {
    }
}
