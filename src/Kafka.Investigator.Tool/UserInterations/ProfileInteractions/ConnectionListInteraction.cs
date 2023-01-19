using ConsoleTables;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConnectionListInteraction
    {
        private readonly ProfileRepository _profileRepository;

        public ConnectionListInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public void ListConnections()
        {
            var connections = _profileRepository.GetConnections();

            var consoleTable = new ConsoleTable("Connection", "Default", "Broker", "Username", "SaslMechanism", "SecurityProtocol", "EnableSslCertificateVerification");

            foreach (var p in connections)
                consoleTable.AddRow(p.ConnectionName, p.Default == true ? "***" : "", p.Broker, p.UserName, p.SaslMechanism, p.SecurityProtocol, p.EnableSslCertificateVerification);

            consoleTable.WriteWithOptions();
        }
    }
}
