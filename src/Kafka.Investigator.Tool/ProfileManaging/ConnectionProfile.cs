using Confluent.Kafka;
using Kafka.Investigator.Tool.Util;

namespace Kafka.Investigator.Tool.ProfileManaging
{
    internal class ConnectionProfile
    {
        public ConnectionProfile()
        {

        }

        public ConnectionProfile(string? connectionName,
                       bool? @default,
                       string? broker,
                       string? userName,
                       string? password,
                       bool encryptedPassword = true,
                       SaslMechanism? saslMechanism = null,
                       SecurityProtocol? securityProtocol = null,
                       bool? enableSslCertificateVerification = null)
        {
            ConnectionName = connectionName;
            Default = @default;
            Broker = broker;
            UserName = userName;
            Password = password;
            EncryptedPassword = encryptedPassword;
            SaslMechanism = saslMechanism ?? SaslMechanism.Plain;
            SecurityProtocol = securityProtocol ?? SecurityProtocol.SaslSsl;
            EnableSslCertificateVerification = enableSslCertificateVerification ?? true;

            Validate();
        }

        private void Validate()
        {
            if (string.IsNullOrEmpty(ConnectionName))
                throw new Exception("Connection name is required.");

            if (string.IsNullOrEmpty(Broker))
                throw new Exception("Broker is required.");
        }

        public string? ConnectionName { get; set; }
        public bool? Default { get; set; }
        public string? Broker { get; set; }
        public string? UserName { get; set; }
        public string? Password { get; set; }
        public bool EncryptedPassword { get; set; }
        public SaslMechanism SaslMechanism { get; set; }
        public SecurityProtocol SecurityProtocol { get; set; }
        public bool EnableSslCertificateVerification { get; set; }

        public string GetPlainPassword()
            => EncryptedPassword ? Password.DecryptForUser() : Password;
    }
}
