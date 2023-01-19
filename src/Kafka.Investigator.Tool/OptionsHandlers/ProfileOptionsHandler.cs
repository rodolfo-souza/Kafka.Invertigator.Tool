using ConsoleTables;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations.ProfileInteractions;
using Kafka.Investigator.Tool.Util;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.OptionsHandlers
{
    internal class ProfileOptionsHandler : IRequestHandler<ConnectionAddOptions>,
                                           IRequestHandler<ConnectionDelOptions>,
                                           IRequestHandler<ConnectionListOptions>,
                                           IRequestHandler<SchemaRegistryAddOptions>,
                                           IRequestHandler<SchemaRegistryListOptions>,
                                           IRequestHandler<SchemaRegistryDelOptions>,
                                           IRequestHandler<ConsumerProfileAddOptions>,
                                           IRequestHandler<ConsumerProfileListOptions>,
                                           IRequestHandler<ConsumerProfileDelOptions>
    {
        private readonly ConnectionAddInteraction _connectionAddInteraction;
        private readonly ConnectionDelInteraction _connectionDelInteraction;
        private readonly SchemaRegistryAddInteraction _schemaRegistryAddInteraction;
        private readonly SchemaRegistryDelInteraction _schemaRegistryDelInteraction;
        private readonly ConsumerProfileAddInteraction _consumerProfileAddInteraction;
        private readonly ConsumerProfileDelInteraction _consumerProfileDelInteraction;
        private readonly ProfileRepository _profileRepository;

        public ProfileOptionsHandler(ConnectionAddInteraction connectionAddInteraction, ConnectionDelInteraction connectionDelInteraction, SchemaRegistryAddInteraction schemaRegistryAddInteraction, SchemaRegistryDelInteraction schemaRegistryDelInteraction, ConsumerProfileAddInteraction consumerProfileAddInteraction, ConsumerProfileDelInteraction consumerProfileDelInteraction, ProfileRepository profileRepository)
        {
            _connectionAddInteraction = connectionAddInteraction;
            _connectionDelInteraction = connectionDelInteraction;
            _schemaRegistryAddInteraction = schemaRegistryAddInteraction;
            _schemaRegistryDelInteraction = schemaRegistryDelInteraction;
            _consumerProfileAddInteraction = consumerProfileAddInteraction;
            _consumerProfileDelInteraction = consumerProfileDelInteraction;
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(ConnectionAddOptions profileAddOptions, CancellationToken cancellationToken)
        {
            _connectionAddInteraction.AddConnection();

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConnectionDelOptions connectionDelOptions, CancellationToken cancellationToken)
        {
            _connectionDelInteraction.DelConnection(connectionDelOptions);

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConnectionListOptions connectionListOptions, CancellationToken cancellationToken)
        {
            var connections = _profileRepository.GetConnections();

            var consoleTable = new ConsoleTable("Connection", "Default", "Broker", "Username", "SaslMechanism", "SecurityProtocol", "EnableSslCertificateVerification");

            foreach (var p in connections)
                consoleTable.AddRow(p.ConnectionName, p.Default == true ? "***" : "", p.Broker, p.UserName, p.SaslMechanism, p.SecurityProtocol, p.EnableSslCertificateVerification);

            consoleTable.WriteWithOptions("Connections List");

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(SchemaRegistryAddOptions schemaRegistryAddOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryAddInteraction.AddSchemaRegistry();

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(SchemaRegistryListOptions schemaRegistryListOptions, CancellationToken cancellationToken)
        {
            var schemaRegistries = _profileRepository.GetSchemaRegistries();

            var consoleTable = new ConsoleTable("Schema Name", "Default", "Url", "Username");

            foreach (var s in schemaRegistries)
                consoleTable.AddRow(s.SchemaRegistryName, s.Default == true ? "***" : "", s.Url, s.UserName);

            consoleTable.WriteWithOptions("Schema Registry List");

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(SchemaRegistryDelOptions schemaRegistryDelOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryDelInteraction.DelSchemaRegistry(schemaRegistryDelOptions);

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConsumerProfileAddOptions consumerAddOptions, CancellationToken cancellationToken)
        {
            _consumerProfileAddInteraction.AddProfile(consumerAddOptions);

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConsumerProfileListOptions consumerProfileListOptions, CancellationToken cancellationToken)
        {
            var consumerProfiles = _profileRepository.GetConsumerProfiles();

            var consoleTable = new ConsoleTable("Name", "Connection", "Topic", "GroupId", "AutooOffsetReset", "Use Schema Registry?", "SchemaRegistry");

            foreach (var c in consumerProfiles)
                consoleTable.AddRow(c.ConsumerName, c.ConnectionName, c.TopicName, c.GroupId, c.AutoOffsetReset, c.UseSchemaRegistry, c.SchemaRegistryName);

            consoleTable.WriteWithOptions("Consumer Profile List");

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConsumerProfileDelOptions consumerProfileDelOptions, CancellationToken cancellationToken)
        {
            _consumerProfileDelInteraction.DelConsumerProfile(consumerProfileDelOptions);

            return Task.FromResult(Unit.Value);
        }
    }
}
