using ConsoleTables;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.Util;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ProfilePrintServices
    {
        internal static void PrintConnections(IReadOnlyList<ConnectionProfile> connections)
        {
            var consoleTable = new ConsoleTable("Connection", "Default", "Broker", "Username", "Encrypted Password", "SaslMechanism", "SecurityProtocol", "EnableSslCertificateVerification");

            foreach (var p in connections)
                consoleTable.AddRow(p.ConnectionName, p.Default == true ? "***" : "", p.Broker, p.UserName, p.EncryptedPassword, p.SaslMechanism, p.SecurityProtocol, p.EnableSslCertificateVerification);

            consoleTable.WriteWithOptions("Connections List");
        }

        internal static void PrintSchemaRegistries(IReadOnlyList<SchemaRegistryProfile> schemaRegistries)
        {
            var consoleTable = new ConsoleTable("Schema Name", "Default", "Url", "Username", "Encrypted Password");

            foreach (var s in schemaRegistries)
                consoleTable.AddRow(s.SchemaRegistryName, s.Default == true ? "***" : "", s.Url, s.UserName, s.EncryptedPassword);

            consoleTable.WriteWithOptions("Schema Registry List");
        }

        internal static void PrintConsumerProfiles(IReadOnlyList<ConsumerProfile> consumerProfiles)
        {
            var consoleTable = new ConsoleTable("Name", "Connection", "Topic", "GroupId", "AutooOffsetReset", "Use Schema Registry?", "SchemaRegistry");

            foreach (var c in consumerProfiles)
                consoleTable.AddRow(c.ConsumerName, c.ConnectionName, c.TopicName, c.GroupId, c.AutoOffsetReset, c.UseSchemaRegistry, c.SchemaRegistryName);

            consoleTable.WriteWithOptions("Consumer Profile List");
        }
    }
}
