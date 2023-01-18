using Confluent.SchemaRegistry;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.KafkaObjects
{
    internal class InvestigatorSchemaRegistryBuilder
    {
        private readonly ProfileRepository _profileRepository;

        public InvestigatorSchemaRegistryBuilder(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public ISchemaRegistryClient BuildSchemaRegistryClient(ConsumeStartOption consumeStartOptions)
        {
            var schemaProfile = GetSchemaRegistryProfile(consumeStartOptions);

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                RequestTimeoutMs = (30 * 1000),
                Url = schemaProfile.Url,
                BasicAuthUserInfo = $"{schemaProfile.UserName}:{schemaProfile.Password}"
            };

            return new CachedSchemaRegistryClient(schemaRegistryConfig);
        }

        private SchemaRegistryProfile GetSchemaRegistryProfile(ConsumeStartOption consumeStartOptions)
        {
            SchemaRegistryProfile schemaRegistryProfile = null;

            if (!string.IsNullOrEmpty(consumeStartOptions.SchemaRegistryName))
            {
                schemaRegistryProfile = _profileRepository.GetSchemaRegistry(consumeStartOptions.SchemaRegistryName);

                if (schemaRegistryProfile == null)
                    throw new Exception($"SchemaRegistry name [{consumeStartOptions.SchemaRegistryName}] not found.");
            }
            else // default connection
            {
                schemaRegistryProfile = _profileRepository.GetSchemaRegistries().FirstOrDefault(c => c.Default == true);

                if (schemaRegistryProfile == null)
                    throw new Exception($"There's no default schema registry configured. Enter a schema registry name or create a default.");
            }

            UserInteractionsHelper.WriteInformation($"Using schema registry [{schemaRegistryProfile.SchemaRegistryName}]");

            return schemaRegistryProfile;
        }
    }
}
