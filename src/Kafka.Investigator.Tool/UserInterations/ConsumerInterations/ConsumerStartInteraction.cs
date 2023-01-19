using Confluent.Kafka;
using Confluent.SchemaRegistry;
using ConsoleTables;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.Util;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerStartInteraction
    {
        private readonly InvestigatorConsumerBuilder _consumerBuilder;
        private readonly InvestigatorSchemaRegistryBuilder _schemaRegistryBuilder;

        public ConsumerStartInteraction(InvestigatorConsumerBuilder consumerBuilder, InvestigatorSchemaRegistryBuilder schemaRegistryBuilder)
        {
            _consumerBuilder = consumerBuilder;
            _schemaRegistryBuilder = schemaRegistryBuilder;
        }

        public void StartConsume(ConsumeStartOption consumeStartOptions, CancellationToken cancellationToken)
        {
            try
            {
                PrintConsumerData(consumeStartOptions);

                var consumer = CreateConsumer(consumeStartOptions);
                var usingSchemaRegistry = TryCreateSchemaRegistry(consumeStartOptions, out ISchemaRegistryClient schemaRegistry);

                UserInteractionsHelper.WriteDebug("Starting consumer...");

                while (!cancellationToken.IsCancellationRequested)
                {
                    PrintConsumerCurrentAssignment(consumer);

                    UserInteractionsHelper.WriteDebug("Waiting for new messages (timeout 10s)...");

                    var consumerResult = consumer.Consume(TimeSpan.FromSeconds(10));

                    if (consumerResult == null)
                    {
                        UserInteractionsHelper.WriteWarning("Nothing returned from broker.");
                        continue;
                    }

                    UserInteractionsHelper.WriteSuccess("===>>> Message received.");

                    bool isKeyAvro = IsAvroMessage(consumerResult.Message.Key, out int? keySchemaId);
                    bool isValueAvro = IsAvroMessage(consumerResult.Message.Value, out int? valueSchemaId);
                    
                    PrintConsumerResultData(consumerResult, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

                    PrintRawMessage(consumerResult);

                    if (usingSchemaRegistry && (isKeyAvro || isValueAvro))
                        PrintAvroSchemas(schemaRegistry, keySchemaId, valueSchemaId);

                    var userOption = RequestUserNextAction();
                    bool stopConsumer = false;

                    switch (userOption)
                    {
                        case 1:
                            UserInteractionsHelper.WriteWarning("Confirm COMMIT? Y/N");
                            if (Console.ReadLine().ToUpper() == "Y")
                            {
                                consumer.Commit(consumerResult);
                                UserInteractionsHelper.WriteInformation("Message commited.");
                            }
                            break;
                        case 2:
                            // TODO: Impplementar 
                            UserInteractionsHelper.WriteError("NOT IMPLEMENTED");
                            break;
                        case 3:
                            continue;
                        case 4:
                            stopConsumer = true;
                            break;
                    }

                    if (stopConsumer)
                        break;

                    UserInteractionsHelper.WriteInformation("Continue consuming? Y/N");
                    if (Console.ReadLine().ToUpper() != "Y")
                        break;
                }

                UserInteractionsHelper.WriteInformation("Consumer finished");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error: " + ex.Message);
            }
        }

        private static void PrintConsumerResultData(ConsumeResult<byte[], byte[]> consumerResult, bool isKeyAvro, int? keySchemaId, bool isValueAvro, int? valueSchemaId)
        {
            var consoleTable = new ConsoleTable("Partition", "Offset", "Is Key Avro?", "Key SchemaId", "Is Value Avro?", "Value SchemaId");

            consoleTable.AddRow(consumerResult.Partition.Value, consumerResult.Offset.Value, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

            consoleTable.Write();
        }

        private static void PrintRawMessage(ConsumeResult<byte[], byte[]> consumerResult)
        {
            var rawMessageTable = new ConsoleTable("-", "Raw (Avro Message will be unreadable)");
            var rawKey = Encoding.UTF8.GetString(consumerResult.Message.Key);
            rawMessageTable.AddRow("Key", rawKey.Limit(100, " [more...]"));

            var rawValue = Encoding.UTF8.GetString(consumerResult.Message.Value);
            rawMessageTable.AddRow("Value", rawValue.Limit(100, " [more...]"));

            rawMessageTable.Write();
        }

        private static void PrintConsumerData(ConsumeStartOption consumeStartOptions)
        {
            var consoleTable = new ConsoleTable("Topic", "GroupId");
            consoleTable.Configure(c => c.EnableCount = false);

            consoleTable.AddRow(consumeStartOptions.TopicName, consumeStartOptions.GroupId);
            consoleTable.Write();
        }

        private IConsumer<byte[], byte[]> CreateConsumer(ConsumeStartOption consumeStartOptions)
        {
            var consumer = _consumerBuilder.BuildConsumer(consumeStartOptions);
            UserInteractionsHelper.WriteInformation("Consumer created...");

            return consumer;
        }

        private bool TryCreateSchemaRegistry(ConsumeStartOption consumeStartOptions, out ISchemaRegistryClient schemaRegistryClient)
        {
            schemaRegistryClient = default;

            if (!consumeStartOptions.UseSchemaRegistry)
            {
                UserInteractionsHelper.WriteDebug("Not using schema registry.");
                return false;
            }

            try
            {
                schemaRegistryClient = _schemaRegistryBuilder.BuildSchemaRegistryClient(consumeStartOptions);
                return true;
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteWarning("Error creating schema registry client: " + ex.Message);
                UserInteractionsHelper.WriteWarning("Consume will continue without schema registry information.");
                return false;
            }
        }

        private static bool IsAvroMessage(byte[] messageBytes, out int? schemaId)
        {
            // Constant used for AvroSerializer.
            byte MagicByte = 0;

            schemaId = null;

            if (messageBytes == null || messageBytes.Length == 0)
                return false;

            using (var stream = new MemoryStream(messageBytes))
            using (var reader = new BinaryReader(stream))
            {
                var firstByte = reader.ReadByte();

                if (firstByte != MagicByte)
                    return false;

                var schemaIdByte = reader.ReadInt32();

                schemaId = IPAddress.NetworkToHostOrder(schemaIdByte);

                return true;
            }

            return false;
        }

        private static void PrintConsumerCurrentAssignment(IConsumer<byte[], byte[]> consumer)
        {
            var consoleTable = new ConsoleTable("Partition", "Offset");
            consoleTable.Options.EnableCount = false;

            foreach (var assignment in consumer.Assignment)
            {
                var watermark = consumer.GetWatermarkOffsets(assignment);
                var partitionOffset = consumer.Position(assignment);
                consoleTable.AddRow(assignment.Partition.Value, partitionOffset.Value);
            }

            UserInteractionsHelper.WriteDebug("Current consumer assignment: ");

            consoleTable.Write();
        }

        private static void PrintAvroSchemas(ISchemaRegistryClient schemaRegistry, int? keySchemaId, int? valueSchemaId)
        {
            UserInteractionsHelper.WriteInformation("SchemaRegistry information:");
            var consoleTable = new ConsoleTable("-", "SchemaId", "Schema");

            if (keySchemaId != null)
            {
                var schema = GetSchema(schemaRegistry, keySchemaId.Value);

                consoleTable.AddRow("Key", keySchemaId, schema.Limit(100, " [more...]"));
            }
            
            if (valueSchemaId != null)
            {
                var schema = GetSchema(schemaRegistry, valueSchemaId.Value);

                consoleTable.AddRow("Value", valueSchemaId, schema.Limit(100, " [more...]"));
            }

            consoleTable.Write();
        }

        private static string GetSchema(ISchemaRegistryClient schemaRegistry, int schemaId)
        {
            try
            {
                var schema = schemaRegistry.GetSchemaAsync(schemaId).Result;

                return schema.SchemaString;
            }
            catch (Exception)
            {
                UserInteractionsHelper.WriteError($"Error trying to get schema for schemaId: {schemaId}");
                return "fail";
            }
        }

        private static int RequestUserNextAction()
        {
            var validOptions = new[] { "1", "2", "3", "4" };

            while(true)
            {
                UserInteractionsHelper.WriteWarning("What do you want to do with message?");
                UserInteractionsHelper.WriteWarning("1 - Commit message (be careful!)");
                UserInteractionsHelper.WriteWarning("3 - Export message (save as file)");
                UserInteractionsHelper.WriteWarning("3 - Continue consuming (without commit)");
                UserInteractionsHelper.WriteWarning("4 - Finish consumer");

                var userOption = Console.ReadLine();

                if (validOptions.Contains(userOption))
                    return int.Parse(userOption);

                UserInteractionsHelper.WriteError($"Invalid option: [{userOption}]");
            }
            
        }

        
    }
}
