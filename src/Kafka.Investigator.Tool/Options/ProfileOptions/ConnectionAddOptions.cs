using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("connection-add", HelpText = "Add or replace a connection.")]
    internal class ConnectionAddOptions : IRequest
    {
    }
}
