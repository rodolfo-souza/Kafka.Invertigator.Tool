using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("schema-registry-list", HelpText = "List all schema registries configured for user.")]
    internal class SchemaRegistryListOptions : IRequest
    {
    }
}
