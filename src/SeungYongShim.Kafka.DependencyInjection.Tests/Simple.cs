using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using FluentAssertions.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SeungYongShim.Kafka.DependencyInjection.Abstractions;
using Xunit;

namespace SeungYongShim.Kafka.DependencyInjection.Tests
{
    public class KafkaSpec
    {
        [Fact]
        public async void Simple()
        {
            var bootstrapServers = "localhost:9092";
            var topicName = "kafka.spec.simple.test";
            var groupId = "unittest";
            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .ConfigureServices(services =>
                    {
                        services.AddSingleton(sp => new ActivitySource("test"));
                        services.AddTransient<KafkaConsumer>();
                        services.AddSingleton(sp => new KafkaConsumerMessageTypes(new[] { typeof(object) }));
                        services.AddSingleton(sp => new KafkaConfig(bootstrapServers, TimeSpan.FromSeconds(10)));
                    })
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

            var channel = Channel.CreateUnbounded<Sample>();

            consumer.Run(groupId, new[] { topicName }, comm =>
            {
                switch (comm)
                {
                    case Commitable<Sample> m:
                        channel.Writer.TryWrite(m.Body);
                        break;
                    default:
                        throw new ApplicationException();
                }
            });

            var cts = new CancellationTokenSource(2.Seconds());
            var value = await channel.Reader.ReadAsync(cts.Token);

            await host.StopAsync();
        }
    }
}
