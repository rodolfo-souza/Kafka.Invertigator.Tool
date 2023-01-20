using Confluent.Kafka;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.ProfileManaging;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerStartRequest
    {
        public string TopicName { get; set; }

        public string GroupId { get; set; }

        public string ConnectionName { get; set; }

        public AutoOffsetReset AutoOffset { get; set; }

        public bool UseSchemaRegistry { get; set; }

        public string SchemaRegistryName { get; set; }

        public ConsumerStartRequest(ConsumerProfile consumerProfile)
        {
            ConnectionName = consumerProfile.ConnectionName;
            TopicName = consumerProfile.TopicName;
            GroupId = consumerProfile.GroupId;
            AutoOffset = consumerProfile.AutoOffsetReset;
            UseSchemaRegistry = consumerProfile.UseSchemaRegistry;
            SchemaRegistryName = consumerProfile.SchemaRegistryName;
        }

        public ConsumerStartRequest(ConsumerStartOptions consumeStartOptions)
        {
            ConnectionName = consumeStartOptions.ConnectionName;
            TopicName = consumeStartOptions.TopicName;
            GroupId = consumeStartOptions.GroupId;
            AutoOffset = consumeStartOptions.AutoOffset;
            UseSchemaRegistry = consumeStartOptions.UseSchemaRegistry;
            SchemaRegistryName = consumeStartOptions.SchemaRegistryName;
        }
    }
}
