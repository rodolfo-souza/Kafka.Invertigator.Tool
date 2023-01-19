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

        public void StartConsume(ConsumerStartRequest consumerStartRequest, CancellationToken cancellationToken)
        {
            try
            {
                var consumer = CreateConsumer(consumerStartRequest);
                var usingSchemaRegistry = TryCreateSchemaRegistry(consumerStartRequest, out ISchemaRegistryClient schemaRegistry);

                if (UserInteractionsHelper.RequestYesNoResponse("Confirm consumer start?") != "Y")
                { 
                    UserInteractionsHelper.WriteWarning("Operation aborted.");
                    return;
                }
                
                UserInteractionsHelper.WriteSuccess("Starting consumer...");

                while (!cancellationToken.IsCancellationRequested)
                {
                    PrintConsumerCurrentAssignment(consumer);

                    UserInteractionsHelper.WriteDebug($"Waiting for new messages from topic [{consumerStartRequest.TopicName}] (timeout 10s)...");

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
                        case 1: // Commit
                            if (UserInteractionsHelper.RequestYesNoResponse("Confirm COMMIT?") == "Y")
                            {
                                consumer.Commit(consumerResult);
                                UserInteractionsHelper.WriteSuccess("Message commited.");
                            }
                            break;
                        case 2: // Save Message
                            SaveMessage(consumerResult.Message);
                            break;
                        case 3: // Continue
                            continue;
                        case 4: // Stop Consumer
                            stopConsumer = true;
                            break;
                    }

                    if (stopConsumer)
                        break;

                    if (UserInteractionsHelper.RequestYesNoResponse("Continue consuming?") != "Y")
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

            consoleTable.WriteWithOptions(title: "Message result", color: ConsoleColor.Blue);
        }

        private static void PrintRawMessage(ConsumeResult<byte[], byte[]> consumerResult)
        {
            var rawMessageTable = new ConsoleTable("-", "Raw (Avro Message will be unreadable)");
            var rawKey = Encoding.UTF8.GetString(consumerResult.Message.Key);
            rawMessageTable.AddRow("Key", rawKey.Limit(100, " [more...]"));

            var rawValue = Encoding.UTF8.GetString(consumerResult.Message.Value);
            rawMessageTable.AddRow("Value", rawValue.Limit(100, " [more...]"));

            rawMessageTable.WriteWithOptions(title: "Raw Message");
        }

        private IConsumer<byte[], byte[]> CreateConsumer(ConsumerStartRequest consumeStartOptions)
        {
            var consumer = _consumerBuilder.BuildConsumer(consumeStartOptions);
            UserInteractionsHelper.WriteInformation("Consumer created...");

            return consumer;
        }

        private bool TryCreateSchemaRegistry(ConsumerStartRequest consumeStartOptions, out ISchemaRegistryClient schemaRegistryClient)
        {
            schemaRegistryClient = default;

            if (!consumeStartOptions.UseSchemaRegistry)
            {
                UserInteractionsHelper.WriteDebug("Not using schema registry.");
                return false;
            }

            try
            {
                schemaRegistryClient = _schemaRegistryBuilder.BuildSchemaRegistryClient(consumeStartOptions.SchemaRegistryName);
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

            consoleTable.WriteWithOptions(title: "Current consumer assignment");
        }

        private static void PrintAvroSchemas(ISchemaRegistryClient schemaRegistry, int? keySchemaId, int? valueSchemaId)
        {
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

            consoleTable.WriteWithOptions(title: "SchemaRegistry information");
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
                UserInteractionsHelper.WriteWithColor("What do you want to do with message?", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("1 - Commit message (be careful!)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("2 - Export message (save as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("3 - Continue consuming (without commit)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("4 - Finish consumer", ConsoleColor.Yellow);

                var userOption = Console.ReadLine();

                if (validOptions.Contains(userOption))
                    return int.Parse(userOption);

                UserInteractionsHelper.WriteError($"Invalid option: [{userOption}]");
            }
            
        }

        private static void SaveMessage(Message<byte[], byte[]> message)
        {
            var stopAsk = false;
            while(!stopAsk)
            {
                try
                {
                    var applicationData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
                    var directory = UserInteractionsHelper.RequestInput<string>($"Inform path (default {applicationData})");

                    if (string.IsNullOrEmpty(directory))
                        directory = applicationData;

                    if (!Directory.Exists(directory))
                        Directory.CreateDirectory(directory);

                    var filePrefix = UserInteractionsHelper.RequestInput<string>("Inform file prefix (ex.: prefix 'abc' will create 'abc-key' and 'abc-value' files)");

                    var keyFilePath = Path.Combine(directory, filePrefix, "-key");
                    var valueFilePath = Path.Combine(directory, filePrefix, "-value");

                    File.WriteAllBytes(keyFilePath, message.Key);
                    File.WriteAllBytes(valueFilePath, message.Value);

                    UserInteractionsHelper.WriteSuccess($"Message exported sucessfully to: ");
                    UserInteractionsHelper.WriteSuccess(keyFilePath);
                    UserInteractionsHelper.WriteSuccess(valueFilePath);

                    stopAsk = true;
                }
                catch (Exception ex)
                {
                    UserInteractionsHelper.WriteError(ex.Message);

                    if (UserInteractionsHelper.RequestYesNoResponse("Try again?") != "Y")
                        stopAsk = true;
                }
            }
        }
    }
}
