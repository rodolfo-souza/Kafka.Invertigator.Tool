using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class SchemaRegistryAddInteraction : IRequestHandler<SchemaRegistryAddOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public SchemaRegistryAddInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(SchemaRegistryAddOptions request, CancellationToken cancellationToken)
        {
            AddSchemaRegistry();

            return Task.FromResult(Unit.Value);
        }

        public void AddSchemaRegistry()
        {
            try
            {
                UserInteractionsHelper.WriteInformation("Add Schema Registry");
                var schemaRegistryName = UserInteractionsHelper.RequestInput<string>("Schema Registry Name");
                var setAsDefaultSchemaRegistry = UserInteractionsHelper.RequestInput<bool>("Set as default schema registry? true/false");
                var url = UserInteractionsHelper.RequestInput<string>("Schema Registry Url");
                var userName = UserInteractionsHelper.RequestInput<string>("UserName");
                var password = UserInteractionsHelper.RequestInput<string>("Password");

                var schemaRegistryProfile = new SchemaRegistryProfile(schemaRegistryName, setAsDefaultSchemaRegistry, url, userName, password);

                schemaRegistryProfile.Validate();

                var existingSchema = _profileRepository.GetSchemaRegistry(schemaRegistryName);

                if (existingSchema != null)
                {
                    var response = UserInteractionsHelper.RequestYesNoResponse($"Already exists a Schema Registry with name [{schemaRegistryName}]. Do you want to replace?");
                    if (response != "Y")
                        return;
                }

                if (setAsDefaultSchemaRegistry)
                    DisableAllDefaultSchemaRegistryFlagsExisting();

                _profileRepository.AddOrReplaceSchemaRegistry(schemaRegistryProfile);

                UserInteractionsHelper.WriteSuccess($"Schema registry [{schemaRegistryProfile.SchemaRegistryName}] created.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error trying to create schema registry: " + ex.Message);
            }
        }

        private void DisableAllDefaultSchemaRegistryFlagsExisting()
        {
            var currentDefaultSchemaRegistries = _profileRepository.GetSchemaRegistries().Where(s => s.Default == true);

            if (currentDefaultSchemaRegistries == null || !currentDefaultSchemaRegistries.Any())
                return;

            foreach (var schema in currentDefaultSchemaRegistries)
            {
                schema.Default = false;
                _profileRepository.AddOrReplaceSchemaRegistry(schema);
            }
        }
    }
}
