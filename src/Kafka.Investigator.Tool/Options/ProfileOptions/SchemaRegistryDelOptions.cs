using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(SchemaRegistryOptions), "del", HelpText = "Delete a Schema Registry.")]
    internal class SchemaRegistryDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of Schema Registry.")]
        public string SchemaRegistryName { get; set; }
    }
}
