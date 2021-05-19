using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Trace;
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

            // arrange
            using var host =
                Host.CreateDefaultBuilder()
                    .UseKafka(new KafkaConfig(bootstrapServers, TimeSpan.FromSeconds(10)))
                    .ConfigureServices(services =>
                    {
                        services.AddOpenTelemetryTracing(builder =>
                        {
                            builder.AddSource("SeungYongShim.OpenTelemetry")
                                   .AddZipkinExporter()
                                   .AddOtlpExporter()
                                   .SetSampler(new AlwaysOnSampler());
                        });
                    })
                    .Build();

            await host.StartAsync();

           

            var consumer = host.Services.GetRequiredService<KafkaConsumer>();
            var producer = host.Services.GetRequiredService<KafkaProducer>();

            consumer.Start(groupId, new[] { topicName });

            await producer.SendAsync(new Sample
            {
                ID = "1"
            }.AddBody(new[] { "Hello", "World" }), topicName);

            var value = await consumer.ConsumeAsync(TimeSpan.FromSeconds(10));

            value.Message.Should().Be(new Sample
            {
                ID = "1"
            }.AddBody(new[] { "Hello", "World" }));

            value.Commit();

            await host.StopAsync();
        }
    }
}
