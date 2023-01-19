using Confluent.SchemaRegistry;
using ConsoleTables;
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

        public ISchemaRegistryClient BuildSchemaRegistryClient(string? schemaRegistryName = null)
        {
            var schemaProfile = GetSchemaRegistryProfile(schemaRegistryName);

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                RequestTimeoutMs = (30 * 1000),
                Url = schemaProfile.Url,
                BasicAuthUserInfo = $"{schemaProfile.UserName}:{schemaProfile.Password}"
            };
            
            PrintSchemaRegistryParameters(schemaProfile);

            return new CachedSchemaRegistryClient(schemaRegistryConfig);
        }

        private SchemaRegistryProfile GetSchemaRegistryProfile(string? schemaRegistryName)
        {
            SchemaRegistryProfile schemaRegistryProfile = null;

            if (!string.IsNullOrEmpty(schemaRegistryName))
            {
                schemaRegistryProfile = _profileRepository.GetSchemaRegistry(schemaRegistryName);

                if (schemaRegistryProfile == null)
                    throw new Exception($"SchemaRegistry name [{schemaRegistryName}] not found.");
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

        private static void PrintSchemaRegistryParameters(SchemaRegistryProfile schemaProfile)
        {
            UserInteractionsHelper.WriteInformation("SchemaRegistry parameters:");
            var consoleTable = new ConsoleTable("SchemaRegistry Parameter", "Value");
            consoleTable.AddRow("Url", schemaProfile.Url);
            consoleTable.AddRow("Username", schemaProfile.UserName);

            consoleTable.Options.EnableCount = false;
            consoleTable.Write(Format.Minimal);
        }
    }
}
