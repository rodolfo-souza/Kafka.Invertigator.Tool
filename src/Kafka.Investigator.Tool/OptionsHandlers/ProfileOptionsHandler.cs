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
    internal class ProfileOptionsHandler : INotificationHandler<ConnectionAddOptions>,
                                           INotificationHandler<ConnectionDelOptions>,
                                           INotificationHandler<ConnectionListOptions>,
                                           INotificationHandler<SchemaRegistryAddOptions>,
                                           INotificationHandler<SchemaRegistryListOptions>,
                                           INotificationHandler<SchemaRegistryDelOptions>,
                                           INotificationHandler<ConsumerProfileAddOptions>,
                                           INotificationHandler<ConsumerProfileListOptions>
    {
        private readonly ConnectionAddInteraction _profileAddInteraction;
        private readonly ConnectionDelInteraction _profileDelInteraction;
        private readonly ConnectionListInteraction _profileListInteraction;
        private readonly SchemaRegistryAddInteraction _schemaRegistryAddInteraction;
        private readonly SchemaRegistryListInteraction _schemaRegistryListInteraction;
        private readonly SchemaRegistryDelInteraction _schemaRegistryDelInteraction;
        private readonly ConsumerProfileAddInteraction _consumerProfileAddInteraction;
        private readonly ProfileRepository _profileRepository;

        public ProfileOptionsHandler(ConnectionAddInteraction profileAddInteraction, ConnectionDelInteraction profileDelInteraction, ConnectionListInteraction profileListInteraction, SchemaRegistryAddInteraction schemaRegistryAddInteraction, SchemaRegistryListInteraction schemaRegistryListInteraction, SchemaRegistryDelInteraction schemaRegistryDelInteraction, ConsumerProfileAddInteraction consumerProfileAddInteraction, ProfileRepository profileRepository)
        {
            _profileAddInteraction = profileAddInteraction;
            _profileDelInteraction = profileDelInteraction;
            _profileListInteraction = profileListInteraction;
            _schemaRegistryAddInteraction = schemaRegistryAddInteraction;
            _schemaRegistryListInteraction = schemaRegistryListInteraction;
            _schemaRegistryDelInteraction = schemaRegistryDelInteraction;
            _consumerProfileAddInteraction = consumerProfileAddInteraction;
            _profileRepository = profileRepository;
        }

        public Task Handle(ConnectionAddOptions profileAddOptions, CancellationToken cancellationToken)
        {
            _profileAddInteraction.AddConnection();

            return Task.CompletedTask;
        }

        public Task Handle(ConnectionDelOptions connectionDelOptions, CancellationToken cancellationToken)
        {
            _profileDelInteraction.DelConnection(connectionDelOptions);

            return Task.CompletedTask;
        }

        public Task Handle(ConnectionListOptions connectionListOptions, CancellationToken cancellationToken)
        {
            _profileListInteraction.ListConnections();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryAddOptions schemaRegistryAddOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryAddInteraction.AddSchemaRegistry();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryListOptions schemaRegistryListOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryListInteraction.ListSchemaRegistries();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryDelOptions schemaRegistryDelOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryDelInteraction.DelSchemaRegistry(schemaRegistryDelOptions);

            return Task.CompletedTask;
        }

        public Task Handle(ConsumerProfileAddOptions consumerAddOptions, CancellationToken cancellationToken)
        {
            _consumerProfileAddInteraction.AddProfile(consumerAddOptions);

            return Task.CompletedTask;
        }

        public Task Handle(ConsumerProfileListOptions consumerProfileListOptions, CancellationToken cancellationToken)
        {
            var consumerProfiles = _profileRepository.GetConsumerProfiles();

            var consoleTable = new ConsoleTable("Name", "Connection", "Topic", "GroupId", "AutooOffsetReset", "Use Schema Registry?", "SchemaRegistry");

            foreach (var c in consumerProfiles)
                consoleTable.AddRow(c.ConsumerName, c.ConnectionName, c.TopicName, c.GroupId, c.AutoOffsetReset, c.UseSchemaRegistry, c.SchemaRegistryName);

            consoleTable.WriteWithOptions("Consumer Profile List");

            return Task.CompletedTask;
        }
    }
}
