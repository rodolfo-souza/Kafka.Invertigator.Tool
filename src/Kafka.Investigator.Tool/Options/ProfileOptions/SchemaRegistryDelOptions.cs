using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("schema-registry-del", HelpText = "Delete a Schema Registry.")]
    internal class SchemaRegistryDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of Schema Registry.")]
        public string SchemaRegistryName { get; set; }
    }
}
