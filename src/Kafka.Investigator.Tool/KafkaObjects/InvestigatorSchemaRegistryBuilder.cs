﻿using Confluent.SchemaRegistry;
using ConsoleTables;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
using Kafka.Investigator.Tool.Util;

namespace Kafka.Investigator.Tool.KafkaObjects
{
    internal class InvestigatorSchemaRegistryBuilder
    {
        private readonly ProfileRepository _profileRepository;

        public InvestigatorSchemaRegistryBuilder(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public ISchemaRegistryClient BuildSchemaRegistryClient(string? schemaRegistryName = null, bool printSchemaRegistryParameters = true)
        {
            var schemaProfile = GetSchemaRegistryProfile(schemaRegistryName);

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo,
                RequestTimeoutMs = (30 * 1000),
                Url = schemaProfile.Url,
                BasicAuthUserInfo = $"{schemaProfile.UserName}:{schemaProfile.GetPlainPassword()}"
            };
            
            if (printSchemaRegistryParameters)
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
            var consoleTable = new ConsoleTable("Parameter", "Value");
            consoleTable.AddRow("Url", schemaProfile.Url);
            consoleTable.AddRow("Username", schemaProfile.UserName);

            consoleTable.WriteWithOptions(title: "SchemaRegistry parameters", format: Format.Minimal);
        }
    }
}
