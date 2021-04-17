using System;
using Google.Protobuf;

namespace SeungYongShim.Kafka
{
    public record Commitable(IMessage Body, string Key, Action Commit);
}
