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
                    
                    PrintConsumerResultData(consumerResult);

                    bool isKeyAvro = IsAvroMessage(consumerResult.Message.Key, out int? keySchemaId);
                    bool isValueAvro = IsAvroMessage(consumerResult.Message.Value, out int? valueSchemaId);

                    PrintRawMessagePreview(consumerResult, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

                    if (usingSchemaRegistry && (isKeyAvro || isValueAvro))
                        PrintAvroSchemas(schemaRegistry, keySchemaId, valueSchemaId);

                    // User actions
                    bool stopConsumer = false;
                    bool readNext = false;
                    while (!stopConsumer && !readNext)
                    {
                        var userOption = RequestUserNextAction();

                        switch (userOption)
                        {
                            case 1: // Continue
                                readNext = true;
                                break;
                            case 2: // Print Message Key
                                UserInteractionsHelper.WriteInformation("Raw Message Key");
                                Console.WriteLine(GetRawValue(consumerResult.Message.Key));
                                break;
                            case 3: // Pring Message Value
                                UserInteractionsHelper.WriteInformation("Raw Message Value");
                                Console.WriteLine(GetRawValue(consumerResult.Message.Value));
                                break;
                            case 4: // Commit Message
                                if (UserInteractionsHelper.RequestYesNoResponse("Confirm COMMIT?") == "Y")
                                {
                                    consumer.Commit(consumerResult);
                                    UserInteractionsHelper.WriteSuccess("Message commited.");
                                }
                                break;
                            case 5: // Save Message
                                SaveMessage(consumerResult.Message);
                                break;
                            case 6: // Stop Consumer
                                stopConsumer = true;
                                break;
                        }
                    }

                    if (stopConsumer)
                        break;
                }

                UserInteractionsHelper.WriteInformation("Consumer finished");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error: " + ex.Message);
            }
        }

        private static void PrintConsumerResultData(ConsumeResult<byte[], byte[]> consumerResult)
        {
            var consoleTable = new ConsoleTable("Partition", "Offset");

            consoleTable.AddRow(consumerResult.Partition.Value, consumerResult.Offset.Value);

            consoleTable.WriteWithOptions(title: "Message result", color: ConsoleColor.Blue);
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
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/59e0243d8bf6e8456b4c9f853c926cf449e89ac8/src/Confluent.SchemaRegistry.Serdes.Avro/Constants.cs
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

        private static string GetRawValue(byte[] messagePart)
        {
            if (messagePart is null)
                return "<null>";
            
            return Encoding.UTF8.GetString(messagePart);
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

            if (!consumer.Assignment.Any())
                consoleTable.AddRow("[none]", "Waiting for broker (server) assignment...");

            consoleTable.WriteWithOptions(title: "Current consumer assignment");
        }

        private static void PrintRawMessagePreview(ConsumeResult<byte[], byte[]> consumerResult, bool isKeyAvro, int? keySchemaId, bool isValueAvro, int? valueSchemaId)
        {
            var rawKey = GetRawValue(consumerResult.Message.Key);
            var rawValue = GetRawValue(consumerResult.Message.Value);

            var rawMessageTable = new ConsoleTable("-", "Avro", "SchemaId", "Raw Preview (Avro values are unreadable)");

            rawMessageTable.AddRow("Key", isKeyAvro, keySchemaId, rawKey.Limit(150, " [more...]"));
            rawMessageTable.AddRow("Value", isValueAvro, valueSchemaId, rawValue.Limit(150, " [more...]"));

            rawMessageTable.WriteWithOptions(title: "Raw Message Preview", color: ConsoleColor.Blue);
        }

        private static void PrintAvroSchemas(ISchemaRegistryClient schemaRegistry, int? keySchemaId, int? valueSchemaId)
        {
            var consoleTable = new ConsoleTable("-", "SchemaId", "Schema");

            if (keySchemaId != null)
            {
                var schema = GetSchema(schemaRegistry, keySchemaId.Value);

                consoleTable.AddRow("Key", keySchemaId, schema.Limit(150, " [more...]"));
            }
            
            if (valueSchemaId != null)
            {
                var schema = GetSchema(schemaRegistry, valueSchemaId.Value);

                consoleTable.AddRow("Value", valueSchemaId, schema.Limit(150, " [more...]"));
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
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError($"Error trying to get schema for schemaId: {schemaId}: " + ex.Message);
                return $"fail: {ex.Message}";
            }
        }

        private static int RequestUserNextAction()
        {
            var validOptions = new[] { "1", "2", "3", "4", "5", "6" };

            while(true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> MESSAGE OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("1 - Continue consuming (without commit)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("2 - View full Message Key", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("3 - View full Message Value", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("4 - Commit message (be careful!)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("5 - Export message (save as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("6 - Finish consumer", ConsoleColor.Yellow);

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
                    var defaultExportPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
                    defaultExportPath = Path.Combine(defaultExportPath, "kafkainvestigator_messages");

                    var selectedDirectory = UserInteractionsHelper.RequestInput<string>($"Inform path (default {defaultExportPath})");

                    if (string.IsNullOrEmpty(selectedDirectory))
                        selectedDirectory = defaultExportPath;

                    if (!Directory.Exists(selectedDirectory))
                        Directory.CreateDirectory(selectedDirectory);

                    var filePrefix = UserInteractionsHelper.RequestInput<string>("Inform file prefix (ex.: prefix 'abc' will create 'abc-key' and 'abc-value' files)");

                    var keyFilePath = Path.Combine(selectedDirectory, filePrefix + "-key");
                    var valueFilePath = Path.Combine(selectedDirectory, filePrefix + "-value");

                    if (File.Exists(keyFilePath))
                    {
                        var replace = UserInteractionsHelper.RequestYesNoResponse($"File {keyFilePath} already exists. Replace?");
                        if (replace != "Y")
                            throw new Exception("Operation cancelled by user.");
                    }

                    if (File.Exists(valueFilePath))
                    {
                        var replace = UserInteractionsHelper.RequestYesNoResponse($"File {valueFilePath} already exists. Replace?");
                        if (replace != "Y")
                            throw new Exception("Operation cancelled by user.");
                    }

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

                    if (UserInteractionsHelper.RequestYesNoResponse("[Export message] Try again?") != "Y")
                        stopAsk = true;
                }
            }
        }
    }
}
