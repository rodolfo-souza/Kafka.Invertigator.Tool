using CommandLine;
using Confluent.Kafka;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ConsumerOptions
{
    [Verb("consumer-start", HelpText = "Starts a consumer from parameters (without a consumer profile).")]
    internal class ConsumerStartOptions : IRequest
    {
        [Option('t', "topic", Required = true, HelpText = "Topic name")]
        public string? TopicName { get; set; }

        [Option('g', "group-id", Required = true, HelpText = "Group Id (identifier of consumer in Kafka server)")]
        public string? GroupId { get; set; }
        
        [Option('c', "connection", Required = false, HelpText = "Connection that will be used. If empty, the default connection will be used. View list using connection-list command.")]
        public string? ConnectionName { get; set; }
        
        [Option('o', "offset", Required = false, Default = AutoOffsetReset.Earliest, HelpText = "AutoOffsetReset. Enter 'Latest' or 'Earliest'. Not applicable for consumer-group with existing offset.")]
        public AutoOffsetReset AutoOffset { get; set; }

        [Option('u', "use-schema-registry", Required = false, Default = true, HelpText = "Indicate if use SchemaRegistry to enrich message information.")]
        public bool UseSchemaRegistry { get; set; }
        
        [Option('s', "schema-registry", Required = false, HelpText = "Schema Registry that will be used. If empty, the default schema registry will be used. View list using connection-list command.")]
        public string? SchemaRegistryName { get; set; }
    }
}
