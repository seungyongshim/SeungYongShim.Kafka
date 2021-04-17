using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using FluentAssertions.Extensions;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace SeungYongShim.Kafka.DependencyInjection.Tests
{
    public class KafkaSpec
    {
        [Fact]
        public async Task Simple()
        {
            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.simple.test";
            var groupId = "unittest";
            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .UseKafka(new KafkaConfig(bootstrapServers, TimeSpan.FromSeconds(10)), typeof(Sample))
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

            await producer.SendAsync(new Sample
            {
                ID = "1"
            }.AddBody(new[] { "Hello", "World" }), topicName);

            var cts = new CancellationTokenSource(15.Seconds());
            var value = await channel.Reader.ReadAsync(cts.Token);

            value.Should().Be(new Sample
            {
                ID = "1"
            }.AddBody(new[] { "Hello", "World" }));

            await host.StopAsync();
        }
    }
}
