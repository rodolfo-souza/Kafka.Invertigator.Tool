using CommandLine;
using CommandLine.Text;
using Confluent.Kafka;
using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ConsumerOptions
{
    [SubVerb(typeof(ConsumerOptions), "start", HelpText = "Starts a consumer from parameters (without a consumer profile).")]
    internal class ConsumerStartOptions : IRequest
    {
        [Option('t', "topic", Required = true, HelpText = "Topic name")]
        public string? TopicName { get; set; }

        [Option('g', "group-id", Required = true, HelpText = "Group Id (identifier of consumer in Kafka server)")]
        public string? GroupId { get; set; }
        
        [Option('c', "connection", Required = false, HelpText = "Connection that will be used. If empty, the default connection will be used. View list using connection-list command.")]
        public string? ConnectionName { get; set; }
        
        [Option('o', "offset", Required = false, Default = AutoOffsetReset.Earliest, HelpText = "AutoOffsetReset. Enter 'Latest' or 'Earliest'. Not applicable for consumer-group with existing offset.")]
        public AutoOffsetReset AutoOffsetReset { get; set; }

        [Option('i', "ignore-schema-registry", Required = false, Default = false, HelpText = "Indicates if SchemaRegistry will be used to enrich message information during consume.")]
        public bool IgnoreSchemaRegistry { get; set; }
        
        [Option('s', "schema-registry", Required = false, HelpText = "Schema Registry that will be used. If empty, the default schema registry will be used. View list using connection-list command.")]
        public string? SchemaRegistryName { get; set; }

        [Usage(ApplicationAlias = "kafkai consumer")]
        public static IEnumerable<Example> Examples
        {
            get
            {
                return new List<Example>()
                {
                    new Example("Starts a consumer for a topic using default connection, schema registry and AutoOffsetReset", new ConsumerStartOptions
                    {
                        TopicName = "some-topic",
                        GroupId = "some-group-id"
                    }),
                    new Example("Starts a consumer for a topic", new ConsumerStartOptions 
                    { 
                        AutoOffsetReset = AutoOffsetReset.Latest,
                        TopicName = "some-topic",
                        GroupId = "some-group-id",
                        ConnectionName =  "dev-connection",
                        SchemaRegistryName = "dev-schema",
                        IgnoreSchemaRegistry = false
                    }),
                    
                };
            }
        }
    }
}
