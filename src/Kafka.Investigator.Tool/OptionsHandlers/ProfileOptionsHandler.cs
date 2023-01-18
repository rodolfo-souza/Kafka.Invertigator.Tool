using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.UserInterations.ProfileInteractions;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.OptionsHandlers
{
    internal class ProfileOptionsHandler : INotificationHandler<ConnectionAddOption>,
                                           INotificationHandler<ConnectionDelOption>,
                                           INotificationHandler<ConnectionListOption>,
                                           INotificationHandler<SchemaRegistryAddOption>,
                                           INotificationHandler<SchemaRegistryListOption>,
                                           INotificationHandler<SchemaRegistryDelOption>
    {
        private readonly ConnectionAddInteraction _profileAddInteraction;
        private readonly ConnectionDelInteraction _profileDelInteraction;
        private readonly ConnectionListInteraction _profileListInteraction;
        private readonly SchemaRegistryAddInteraction _schemaRegistryAddInteraction;
        private readonly SchemaRegistryListInteraction _schemaRegistryListInteraction;
        private readonly SchemaRegistryDelInteraction _schemaRegistryDelInteraction;

        public ProfileOptionsHandler(ConnectionAddInteraction profileAddInteraction, ConnectionDelInteraction profileDelInteraction, ConnectionListInteraction profileListInteraction, SchemaRegistryAddInteraction schemaRegistryAddInteraction, SchemaRegistryListInteraction schemaRegistryListInteraction, SchemaRegistryDelInteraction schemaRegistryDelInteraction)
        {
            _profileAddInteraction = profileAddInteraction;
            _profileDelInteraction = profileDelInteraction;
            _profileListInteraction = profileListInteraction;
            _schemaRegistryAddInteraction = schemaRegistryAddInteraction;
            _schemaRegistryListInteraction = schemaRegistryListInteraction;
            _schemaRegistryDelInteraction = schemaRegistryDelInteraction;
        }

        public Task Handle(ConnectionAddOption profileAddOptions, CancellationToken cancellationToken)
        {
            _profileAddInteraction.AddConnection();

            return Task.CompletedTask;
        }

        public Task Handle(ConnectionDelOption connectionDelOptions, CancellationToken cancellationToken)
        {
            _profileDelInteraction.DelConnection(connectionDelOptions);

            return Task.CompletedTask;
        }

        public Task Handle(ConnectionListOption connectionListOptions, CancellationToken cancellationToken)
        {
            _profileListInteraction.ListConnections();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryAddOption schemaRegistryAddOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryAddInteraction.AddSchemaRegistry();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryListOption schemaRegistryListOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryListInteraction.ListSchemaRegistries();

            return Task.CompletedTask;
        }

        public Task Handle(SchemaRegistryDelOption schemaRegistryDelOptions, CancellationToken cancellationToken)
        {
            _schemaRegistryDelInteraction.DelSchemaRegistry(schemaRegistryDelOptions);

            return Task.CompletedTask;
        }
    }
}
