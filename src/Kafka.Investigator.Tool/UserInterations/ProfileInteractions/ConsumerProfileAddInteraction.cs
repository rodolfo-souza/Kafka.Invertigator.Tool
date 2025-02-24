﻿using Confluent.Kafka;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConsumerProfileAddInteraction : IRequestHandler<ConsumerProfileAddOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public ConsumerProfileAddInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(ConsumerProfileAddOptions consumerAddOptions, CancellationToken cancellationToken)
        {
            AddProfile(consumerAddOptions);

            return Task.FromResult(Unit.Value);
        }

        internal void AddProfile(ConsumerProfileAddOptions consumerAddOptions)
        {
            try
            {
                UserInteractionsHelper.WriteInformation("Add Consumer Profile");
                var consumerName = UserInteractionsHelper.RequestInput<string>("Consumer Name (don't use spaces)");
                var topicName = UserInteractionsHelper.RequestInput<string>("Topic Name");
                var groupId = UserInteractionsHelper.RequestInput<string>("GroupId");
                var connectionName = UserInteractionsHelper.RequestInput<string>("Connection Name (empty to use default connection aways)");
                var autoOffsetResetString = UserInteractionsHelper.RequestInput<string>("AutoOffsetReset (empty for default Earliest)");
                var useSchemaRegistry = UserInteractionsHelper.RequestInput<bool?>("Use schema registry (true/false)");
                string schemaRegistryName = null;
                if (useSchemaRegistry == true)
                    schemaRegistryName = UserInteractionsHelper.RequestInput<string>("Schema Registry Name (empty to use default schema registry aways");

                AutoOffsetReset? autoOffsetReset = autoOffsetResetString is null ? null : Enum.Parse<AutoOffsetReset>(autoOffsetResetString);

                var newConsumer = new ConsumerProfile(consumerName, topicName, groupId, connectionName, autoOffsetReset, useSchemaRegistry, schemaRegistryName);

                newConsumer.Validate();

                ValidateConnection(newConsumer);
                
                ValidateSchemaRegistry(schemaRegistryName);

                var existingConsumer = _profileRepository.GetConsumerProfile(consumerName);

                if (existingConsumer != null)
                {
                    var response = UserInteractionsHelper.RequestYesNoResponse($"Already exists a consumer with name [{consumerName}]. Do you want to replace?");
                    if (response != "Y")
                        return;
                }

                _profileRepository.AddOrReplaceConsumerProfile(newConsumer);

                UserInteractionsHelper.WriteSuccess($"Connection [{newConsumer.ConsumerName}] created.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }

        }

        private void ValidateSchemaRegistry(string schemaRegistryName)
        {
            if (!string.IsNullOrEmpty(schemaRegistryName))
            {
                if (_profileRepository.GetSchemaRegistry(schemaRegistryName) == null)
                    throw new Exception($"SchemaRegistry [{schemaRegistryName}] not found.");
            }
        }

        private void ValidateConnection(ConsumerProfile newConsumer)
        {
            if (!string.IsNullOrEmpty(newConsumer.ConnectionName))
            {
                if (_profileRepository.GetConnection(newConsumer.ConnectionName) == null)
                    throw new Exception($"Connection [{newConsumer.ConnectionName}] not found.");
            }
        }
    }
}
