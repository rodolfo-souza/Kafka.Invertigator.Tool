using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(SchemaRegistryOptions), "add", HelpText = "Add or replace a SchemaRegistry configuration for BasicAuthentication.")]
    internal class SchemaRegistryAddOptions : IRequest
    {
    }
}
