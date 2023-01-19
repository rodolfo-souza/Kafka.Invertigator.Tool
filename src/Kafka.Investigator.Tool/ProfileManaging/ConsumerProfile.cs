using Confluent.Kafka;
using Kafka.Investigator.Tool.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.ProfileManaging
{
    internal class ConsumerProfile
    {
        public ConsumerProfile()
        {

        }

        public ConsumerProfile(string consumerName, 
                               string topicName, 
                               string groupId, 
                               string connectionName, 
                               AutoOffsetReset? autoOffsetReset = null, 
                               bool? useSchemaRegistry = null, 
                               string? schemaRegistryName = null)
        {
            ConsumerName = consumerName;
            TopicName = topicName;
            GroupId = groupId;
            ConnectionName = connectionName;
            AutoOffsetReset = autoOffsetReset ?? Confluent.Kafka.AutoOffsetReset.Earliest;
            UseSchemaRegistry = useSchemaRegistry ?? true;
            SchemaRegistryName = schemaRegistryName;
        }

        public string ConsumerName { get; set; }
        
        public string TopicName { get; set; }
        public string GroupId { get; set; }
        public string ConnectionName { get; set; }
        public AutoOffsetReset? AutoOffsetReset { get; set; }
        public bool? UseSchemaRegistry { get; set; }
        public string? SchemaRegistryName { get; set; }

        public void Validate()
        {
            if (ConsumerName.IsNullOrEmptyOrContaisSpaces())
                throw new Exception("Invalid consumer name.");

            if (TopicName.IsNullOrEmptyOrContaisSpaces())
                throw new Exception("Invalid topic name.");

            if (GroupId.IsNullOrEmptyOrContaisSpaces())
                throw new Exception("Invalid GroupId.");

            if (!string.IsNullOrEmpty(ConnectionName) && ConnectionName.ContainSpaces())
                throw new Exception("Invalid connection name.");

            if (UseSchemaRegistry == true && SchemaRegistryName.ContainSpaces())
                throw new Exception("Invalid schema registry.");
        }
    }
}
