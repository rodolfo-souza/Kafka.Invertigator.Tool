using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("connection-list", HelpText = "List all connections configured for user.")]
    internal class ConnectionListOptions : IRequest
    {
    }
}
