using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(SchemaRegistryOptions), "list", HelpText = "List all schema registries configured for user.")]
    internal class SchemaRegistryListOptions : IRequest
    {
    }
}
