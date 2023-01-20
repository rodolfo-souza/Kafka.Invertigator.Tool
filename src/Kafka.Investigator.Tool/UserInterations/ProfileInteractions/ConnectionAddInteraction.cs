using Confluent.Kafka;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConnectionAddInteraction : IRequestHandler<ConnectionAddOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public ConnectionAddInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(ConnectionAddOptions request, CancellationToken cancellationToken)
        {
            AddConnection();

            return Task.FromResult(Unit.Value);
        }

        public void AddConnection()
        {
            try
            {
                UserInteractionsHelper.WriteInformation("Add Connection");
                var connectionName = UserInteractionsHelper.RequestInput<string>("Connection Name (don't use spaces)");
                var setAsDefaultConnection = UserInteractionsHelper.RequestInput<bool>("Set as default connection? true/false");
                var broker = UserInteractionsHelper.RequestInput<string>("Broker");
                var userName = UserInteractionsHelper.RequestInput<string>("UserName");
                var password = UserInteractionsHelper.RequestInput<string>("Password");
                var saslMechanismString = UserInteractionsHelper.RequestInput<string>("SaslMechanism (optional, default: Plain)");
                var securityProtocolString = UserInteractionsHelper.RequestInput<string>("SecurityProtocol (optional, default: SaslSsl)");
                var enableSslCertificateVerification = UserInteractionsHelper.RequestInput<bool>("EnableSslCertificateVerification (optional, default: true)");

                SaslMechanism? saslMechanism = saslMechanismString is null ? null : Enum.Parse<SaslMechanism>(saslMechanismString);
                SecurityProtocol? securityProtocol = securityProtocolString is null ? null : Enum.Parse<SecurityProtocol>(securityProtocolString);

                var newConnection = new ConnectionProfile(connectionName, setAsDefaultConnection, broker, userName, password, saslMechanism, securityProtocol, enableSslCertificateVerification);

                var existingProfile = _profileRepository.GetConnection(connectionName);

                if (existingProfile != null)
                {
                    var response = UserInteractionsHelper.RequestYesNoResponse($"Already exists a connection with name [{connectionName}]. Do you want to replace?");
                    if (response != "Y")
                        return;
                }

                if (setAsDefaultConnection)
                    DisableAllDefaultConnectionsFlagsExisting();

                _profileRepository.AddOrReplaceConnection(newConnection);

                UserInteractionsHelper.WriteSuccess($"Connection [{newConnection.ConnectionName}] created.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error trying to create connection: " + ex.Message);
            }

        }

        private void DisableAllDefaultConnectionsFlagsExisting()
        {
            var currentDefaultConnections = _profileRepository.GetConnections().Where(c => c.Default == true);

            if (currentDefaultConnections == null || !currentDefaultConnections.Any())
                return;

            foreach (var connection in currentDefaultConnections)
            {
                connection.Default = false;
                _profileRepository.AddOrReplaceConnection(connection);
            }
        }
    }
}
