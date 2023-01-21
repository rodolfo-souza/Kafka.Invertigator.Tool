using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerbAttribute(typeof(ConnectionOptions), "list", HelpText = "List all connections configured for user.")]
    internal class ConnectionListOptions : IRequest
    {
    }
}
