using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("schema-registry-add", HelpText = "Add or replace a SchemaRegistry configuration for BasicAuthentication.")]
    internal class SchemaRegistryAddOptions : IRequest
    {
    }
}
