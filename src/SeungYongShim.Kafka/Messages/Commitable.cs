using System;
using SeungYongShim.Kafka.Abstractions;

namespace SeungYongShim.Kafka
{
    public record Commitable<T>(T Body, string Key, Action Commit) : ICommitable;
}
