using System;
using Google.Protobuf;

namespace SeungYongShim.Kafka
{
    public record Commitable(IMessage Message, Action Commit);
}
