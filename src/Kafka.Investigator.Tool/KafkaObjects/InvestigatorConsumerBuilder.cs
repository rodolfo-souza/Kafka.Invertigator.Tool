using Confluent.Kafka;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
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

        public IConsumer<byte[], byte[]> BuildConsumer(ConsumeStartOption consumeStartOption)
        {
            var connectionProfile = GetConnectionProfile(consumeStartOption);

            var consumerConfig = new ConsumerConfig()
            {
                // ConnectionProfile
                SaslMechanism = connectionProfile.SaslMechanism,
                SecurityProtocol = connectionProfile.SecurityProtocol,
                EnableSslCertificateVerification = connectionProfile.EnableSslCertificateVerification,
                BootstrapServers = connectionProfile.Broker,
                SaslUsername = connectionProfile.UserName,
                SaslPassword = connectionProfile.Password,

                // ConsumeStartOptions
                GroupId = consumeStartOption.GrouptId,
                AutoOffsetReset = consumeStartOption.AutoOffset,

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

            var consumerBuilder = new ConsumerBuilder<byte[], byte[]>(consumerConfig)
                                  .SetErrorHandler((message, error) => UserInteractionsHelper.WriteError(error.Reason));

            var consumer = consumerBuilder.Build();

            consumer.Subscribe(consumeStartOption.TopicName);

            return consumer;
        }

        private ConnectionProfile GetConnectionProfile(ConsumeStartOption consumeStartOption)
        {
            ConnectionProfile connectionProfile = null;

            if (!string.IsNullOrEmpty(consumeStartOption.ConnectionName))
            {
                connectionProfile = _profileRepository.GetConnection(consumeStartOption.ConnectionName);

                if (connectionProfile == null)
                    throw new Exception($"Connection name [{consumeStartOption.ConnectionName}] not found.");
            }
            else // default connection
            {
                connectionProfile = _profileRepository.GetConnections().FirstOrDefault(c => c.Default == true);

                if (connectionProfile == null)
                    throw new Exception($"There's no default connection configured. Enter a connection name or create a default connection.");
            }

            UserInteractionsHelper.WriteInformation($"Using connection [{connectionProfile.ConnectionName}]");

            return connectionProfile;
        }
    }
}
