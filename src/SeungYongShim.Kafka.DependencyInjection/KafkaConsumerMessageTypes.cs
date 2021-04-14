using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using Google.Protobuf;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public class KafkaConsumerMessageTypes
    {
        public KafkaConsumerMessageTypes(IList<Type> types)
        {
            GetTypeAll = (from t in types
                          let assembly = Assembly.GetAssembly(t)
                          from type in assembly.GetTypes()
                          where typeof(IMessage).IsAssignableFrom(type)
                          where type.IsInterface is false
                          select (type.FullName, type)).ToImmutableDictionary(x => x.FullName, y => y.type);

            GetParserAll = (from type in GetTypeAll.Values
                            select (type.FullName, type.GetProperty("Parser")
                                                       .GetGetMethod()?
                                                       .Invoke(null, null) as MessageParser))
                           .ToImmutableDictionary(x => x.FullName, x => x.Item2);
        }

        public ImmutableDictionary<string, MessageParser> GetParserAll { get; }
        public ImmutableDictionary<string, Type> GetTypeAll { get; }
    }
}
