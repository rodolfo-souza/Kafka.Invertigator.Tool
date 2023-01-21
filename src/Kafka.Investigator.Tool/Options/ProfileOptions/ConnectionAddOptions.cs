using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(ConnectionOptions), "add", HelpText = "Add or replace a connection.")]
    internal class ConnectionAddOptions : IRequest
    {
    }
}
