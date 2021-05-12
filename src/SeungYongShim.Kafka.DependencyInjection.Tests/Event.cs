using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using FluentAssertions.Extensions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.ProtobufHelper;
using Xunit;

namespace SeungYongShim.Kafka.DependencyInjection.Tests
{
    public class KafkaEventSpec
    {
        [Fact]
        public async Task Event()
        {
            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.event.test";
            var groupId = "unittest";
            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .UseKafka(new KafkaConfig(bootstrapServers, TimeSpan.FromSeconds(10)))
                    .Build();

            await host.StartAsync();

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                try
                {
                    await adminClient.DeleteTopicsAsync(new[]
                    {
                        topicName
                    });
                    await Task.Delay(1000);
                }
                catch (DeleteTopicsException)
                {
                }

                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                {
                    new TopicSpecification
                    {
                        Name = topicName,
                        ReplicationFactor = 1,
                        NumPartitions = 1
                    }
                });
            }

            var consumer = host.Services.GetRequiredService<KafkaConsumer>();
            var producer = host.Services.GetRequiredService<KafkaProducer>();
            var knownTypes = host.Services.GetRequiredService<ProtoKnownTypes>();

            var channel = Channel.CreateUnbounded<IMessage>();

            consumer.Run(groupId, new[] { topicName }, comm =>
            {
                switch (comm)
                {
                    case Commitable m:
                        channel.Writer.TryWrite(m.Body);
                        m.Commit();
                        break;

                    default:
                        throw new ApplicationException();
                }
            });

            await producer.SendAsync(new Event
            {
                TraceId = "111",
                Body = Any.Pack(new Sample
                {
                    ID = "1"
                }.AddBody(new[] { "Hello", "World" }))
            }, topicName);

            var cts = new CancellationTokenSource(15.Seconds());
            var value = await channel.Reader.ReadAsync(cts.Token) as Event;

            var x = knownTypes.Unpack(value.Body);

            x.Should().Be(new Sample
            {
                ID = "1"
            }.Select(x => x.Body.Add(new[] { "Hello", "World" })));

            await host.StopAsync();
        }
    }
}
