using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace SeungYongShim.Kafka
{
    public class KafkaProtobufMessageTypes
    {
        public KafkaProtobufMessageTypes(IEnumerable<Type> types)
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

            var descriptorAll = (from type in GetTypeAll.Values
                                 select (type.GetProperty("Descriptor")
                                             .GetGetMethod()?
                                             .Invoke(null, null) as MessageDescriptor)).ToList();

            

            Registry = TypeRegistry.FromMessages(descriptorAll);
            JsonParser = new JsonParser(JsonParser.Settings.Default.WithTypeRegistry(Registry));

            JsonFormatter = new JsonFormatter(JsonFormatter.Settings.Default.WithTypeRegistry(Registry));
                    

        }

        public ImmutableDictionary<string, MessageParser> GetParserAll { get; }
        public ImmutableDictionary<string, Type> GetTypeAll { get; }
        public TypeRegistry Registry { get; }
        public JsonParser JsonParser { get; }
        public JsonFormatter JsonFormatter { get; }
    }
}
