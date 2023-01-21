using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerbAttribute(typeof(ConnectionOptions), "del", HelpText = "Delete a connection.")]
    internal class ConnectionDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of connection.")]
        public string ConnectionName { get; set; }
    }
}
