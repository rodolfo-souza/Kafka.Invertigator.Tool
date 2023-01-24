using Confluent.SchemaRegistry;
using ConsoleTables;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.Util;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class SchemaRegistryTestInteraction : IRequestHandler<SchemaRegistryTestOptions>
    {
        private readonly ProfileRepository _profileRepository;
        private readonly InvestigatorSchemaRegistryBuilder _schemaRegistryBuilder;

        public SchemaRegistryTestInteraction(ProfileRepository profileRepository, InvestigatorSchemaRegistryBuilder schemaRegistryBuilder)
        {
            _profileRepository = profileRepository;
            _schemaRegistryBuilder = schemaRegistryBuilder;
        }

        public async Task<Unit> Handle(SchemaRegistryTestOptions testOptions, CancellationToken cancellationToken)
        {
            await TestSchemaRegistryAsync(testOptions);

            return Unit.Value;
        }

        private async Task TestSchemaRegistryAsync(SchemaRegistryTestOptions request)
        {
            try
            {
                var schemaRegistryClient = _schemaRegistryBuilder.BuildSchemaRegistryClient(request.SchemaRegistryName);

                var subjects = await TestConnectionAsync(schemaRegistryClient);

                if (request.ListSubjects)
                    PrintSubjects(subjects);

                if (request.SchemaId != null)
                    await GetAndPrintSchemaAsync(schemaRegistryClient, request.SchemaId);
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }
        }

        private static async Task<IEnumerable<string>> TestConnectionAsync(ISchemaRegistryClient schemaRegistryClient)
        {
            try
            {
                UserInteractionsHelper.WriteDebug("Testing schema registry...");

                var subjects = await schemaRegistryClient.GetAllSubjectsAsync();

                UserInteractionsHelper.WriteSuccess($"Schema registry OK. {subjects.Count} subjects found.");

                return subjects;
            }
            catch (Exception ex) 
            {
                throw new Exception("Error testing schema registry client: " + ex.Message);
            }
        }

        private static void PrintSubjects(IEnumerable<string> subjects)
        {
            try
            {
                var listTable = new ConsoleTable("Subjects");

                foreach (var item in subjects)
                    listTable.AddRow(item);

                listTable.WriteWithOptions(enableCount: true, format: Format.Minimal);
            }
            catch (Exception ex)
            {
                throw new Exception("Error trying to list subjects: " + ex.Message);
            }
        }

        private static async Task GetAndPrintSchemaAsync(ISchemaRegistryClient schemaRegistryClient, int? schemaId)
        {
            if (schemaId == null)
                return;

            try
            {
                UserInteractionsHelper.WriteDebug($"Getting schema {schemaId}...");
                var schema = await schemaRegistryClient.GetSchemaAsync(schemaId.Value);

                if (schema == null)
                {
                    UserInteractionsHelper.WriteWarning($"Schema {schemaId} not found.");
                    return;
                }

                UserInteractionsHelper.WriteDebug($"Schema type: {schema.SchemaType}");
                UserInteractionsHelper.WriteDebug("Full schema:");
                Console.WriteLine(schema.SchemaString);
            }
            catch (Exception ex)
            {
                throw new Exception("Error trying to get schema: " + ex.Message);
            }
        }
    }
}
