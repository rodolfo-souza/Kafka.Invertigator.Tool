using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class SchemaRegistryAddInteraction
    {
        private readonly ProfileRepository _profileRepository;

        public SchemaRegistryAddInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public void AddSchemaRegistry()
        {
            try
            {
                var schemaRegistryName = UserInteractionsHelper.RequestInput<string>("SchemaRegistry Name");
                var setAsDefaultSchemaRegistry = UserInteractionsHelper.RequestInput<bool>("Set as default schema registry? true/false");
                var url = UserInteractionsHelper.RequestInput<string>("SchemaRegistry Url");
                var userName = UserInteractionsHelper.RequestInput<string>("UserName");
                var password = UserInteractionsHelper.RequestInput<string>("Password");

                var schemaRegistryProfile = new SchemaRegistryProfile(schemaRegistryName, setAsDefaultSchemaRegistry, url, userName, password);

                schemaRegistryProfile.Validate();

                var existingSchema = _profileRepository.GetSchemaRegistry(schemaRegistryName);

                if (existingSchema != null)
                {
                    UserInteractionsHelper.WriteWarning($"Already exists a SchemaRegistry with name [{schemaRegistryName}]. Do you want to replace? Y/N");
                    if (Console.ReadLine().ToUpper() != "Y")
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
