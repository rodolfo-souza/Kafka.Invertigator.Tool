using Confluent.Kafka;
using ConsoleTables;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using Kafka.Investigator.Tool.Util;

namespace Kafka.Investigator.Tool.KafkaObjects
{
    internal class InvestigatorConsumerBuilder
    {
        private readonly ProfileRepository _profileRepository;

        public InvestigatorConsumerBuilder(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public IConsumer<byte[], byte[]> BuildConsumer(ConsumerStartRequest consumerStartRequest, bool printConsumerParameters = true)
        {
            var connectionProfile = GetConnectionProfile(consumerStartRequest.ConnectionName);

            var consumerConfig = CreateConsumerConfig(connectionProfile, consumerStartRequest.GroupId, consumerStartRequest.AutoOffset);

            if (printConsumerParameters)
                ConsumerPrintServices.PrintConsumerConfig(consumerStartRequest, consumerConfig);

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
            var password = string.IsNullOrEmpty(connectionProfile.GetPlainPassword()) ? " " : connectionProfile.GetPlainPassword();

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
                EnableAutoCommit = false, //!!!!!!!!!
                Acks = Acks.All,
                EnableAutoOffsetStore = false,
                AutoCommitIntervalMs = 10,
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
    }
}
