using System;
using SeungYongShim.Kafka.DependencyInjection.Abstractions;

namespace SeungYongShim.Kafka.DependencyInjection
{
    public record Commitable<T>(T Body, string Key, Action Commit) : ICommitable;
}
