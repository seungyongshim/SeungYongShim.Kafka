using System;

namespace SeungYongShim.Kafka.DependencyInjection.Abstractions
{
    public record Commitable<T>(T Body, string Key, Action Commit) : ICommitable;
}
