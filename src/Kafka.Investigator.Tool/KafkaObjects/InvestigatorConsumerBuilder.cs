using Confluent.Kafka;
using ConsoleTables;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using Kafka.Investigator.Tool.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.KafkaObjects
{
    internal class InvestigatorConsumerBuilder
    {
        private readonly ProfileRepository _profileRepository;

        public InvestigatorConsumerBuilder(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public IConsumer<byte[], byte[]> BuildConsumer(ConsumerStartRequest consumerStartRequest)
        {
            var connectionProfile = GetConnectionProfile(consumerStartRequest.ConnectionName);

            var consumerConfig = CreateConsumerConfig(connectionProfile, consumerStartRequest.GroupId, consumerStartRequest.AutoOffset);

            PrintConsumerConfig(consumerStartRequest, consumerConfig);

            return BuildConsumerForTopic(consumerConfig, consumerStartRequest.TopicName);
        }

        private ConnectionProfile GetConnectionProfile(string connectionName)
        {
            ConnectionProfile connectionProfile = null;

            if (!string.IsNullOrEmpty(connectionName))
            {
                connectionProfile = _profileRepository.GetConnection(connectionName);

                if (connectionProfile == null)
                    throw new Exception($"Connection name [{connectionName}] not found.");
            }
            else // default connection
            {
                connectionProfile = _profileRepository.GetConnections().FirstOrDefault(c => c.Default == true);

                if (connectionProfile == null)
                    throw new Exception($"There's no default connection configured. Create a default connection.");
            }

            UserInteractionsHelper.WriteInformation($"Using connection [{connectionProfile.ConnectionName}]");

            return connectionProfile;
        }

        private static ConsumerConfig CreateConsumerConfig(ConnectionProfile connectionProfile, string groupId, AutoOffsetReset autoOffsetReset)
        {
            var userName = string.IsNullOrEmpty(connectionProfile.UserName) ? " " : connectionProfile.UserName;
            var password = string.IsNullOrEmpty(connectionProfile.Password) ? " " : connectionProfile.Password;

            var consumerConfig = new ConsumerConfig()
            {
                // ConnectionProfile
                SaslMechanism = connectionProfile.SaslMechanism,
                SecurityProtocol = connectionProfile.SecurityProtocol,
                EnableSslCertificateVerification = connectionProfile.EnableSslCertificateVerification,
                BootstrapServers = connectionProfile.Broker,
                SaslUsername = userName,
                SaslPassword = password,

                // ConsumeStartOptions
                GroupId = groupId,
                AutoOffsetReset = autoOffsetReset,

                // Hard code
                ClientId = Environment.MachineName,
                ConnectionsMaxIdleMs = 180000,
                TopicMetadataRefreshIntervalMs = 180000,
                MetadataMaxAgeMs = 210000,
                SocketTimeoutMs = 30000,
                EnableAutoCommit = false,
                Acks = Acks.All,
                EnableAutoOffsetStore = true,
                BrokerAddressFamily = BrokerAddressFamily.V4,
                SocketKeepaliveEnable = true,
            };

            return consumerConfig;
        }

        private static IConsumer<byte[], byte[]> BuildConsumerForTopic(ConsumerConfig consumerConfig, string topicName)
        {
            var consumerBuilder = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                                  .SetErrorHandler((message, error) =>
                                  {
                                      UserInteractionsHelper.WriteError("CONSUMER ERROR: " + error.Reason);
                                  })
                                  .SetPartitionsRevokedHandler((c, partitions) =>
                                  {
                                      UserInteractionsHelper.WriteWarning($"[ConsumerWarning] Group rebalancing occurred.");
                                  });

            var consumer = consumerBuilder.Build();

            consumer.Subscribe(topicName);

            return consumer;
        }

        private static void PrintConsumerConfig(ConsumerStartRequest consumerStartRequest, ConsumerConfig consumerConfig)
        {
            var consoleTable = new ConsoleTable("Parameter", "Value");

            consoleTable.AddRow("Topic", consumerStartRequest.TopicName);
            consoleTable.AddRow("GroupId", consumerConfig.GroupId);

            consoleTable.AddRow("BootstrapServers", consumerConfig.BootstrapServers);
            consoleTable.AddRow("SaslUsername", consumerConfig.SaslUsername);
            consoleTable.AddRow("SaslMechanism", consumerConfig.SaslMechanism);
            consoleTable.AddRow("SecurityProtocol", consumerConfig.SecurityProtocol);
            consoleTable.AddRow("EnableSslCertificateVerification", consumerConfig.EnableSslCertificateVerification);

            consoleTable.AddRow("AutoOffsetReset", consumerConfig.AutoOffsetReset);
            consoleTable.AddRow("EnableAutoCommit", consumerConfig.EnableAutoCommit);
            consoleTable.AddRow("ClientId", consumerConfig.ClientId);
            consoleTable.AddRow("ConnectionsMaxIdleMs", consumerConfig.ConnectionsMaxIdleMs);
            consoleTable.AddRow("TopicMetadataRefreshIntervalMs", consumerConfig.TopicMetadataRefreshIntervalMs);
            consoleTable.AddRow("MetadataMaxAgeMs", consumerConfig.MetadataMaxAgeMs);
            consoleTable.AddRow("SocketTimeoutMs", consumerConfig.SocketTimeoutMs);

            consoleTable.AddRow("Acks", consumerConfig.Acks);
            consoleTable.AddRow("EnableAutoOffsetStore", consumerConfig.EnableAutoOffsetStore);
            consoleTable.AddRow("BrokerAddressFamily", consumerConfig.BrokerAddressFamily);
            consoleTable.AddRow("SocketKeepaliveEnable", consumerConfig.SocketKeepaliveEnable);

            consoleTable.Options.EnableCount = false;
            consoleTable.WriteWithOptions(title: "Consumer Config");
        }
    }
}
