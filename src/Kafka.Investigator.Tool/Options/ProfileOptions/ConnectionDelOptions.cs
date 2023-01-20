using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("connection-del", HelpText = "Delete a connection.")]
    internal class ConnectionDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of connection.")]
        public string ConnectionName { get; set; }
    }
}
