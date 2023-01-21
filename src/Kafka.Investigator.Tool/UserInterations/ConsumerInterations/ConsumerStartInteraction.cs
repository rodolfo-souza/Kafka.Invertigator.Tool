using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Investigator.Tool.KafkaObjects;
using System.Net;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerStartInteraction
    {
        private readonly InvestigatorConsumerBuilder _consumerBuilder;
        private readonly InvestigatorSchemaRegistryBuilder _schemaRegistryBuilder;

        private const int TimeoutSeconds = 10;

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

                var usingSchemaRegistry = TryCreateSchemaRegistryClient(consumerStartRequest, out ISchemaRegistryClient schemaRegistry);
                if (!usingSchemaRegistry)
                    UserInteractionsHelper.WriteWarning("Consume will continue without schema registry information.");

                if (UserInteractionsHelper.RequestYesNoResponse("Start reading messages?") != "Y")
                { 
                    UserInteractionsHelper.WriteWarning("Operation aborted.");
                    return;
                }
                
                UserInteractionsHelper.WriteSuccess("Starting consumer...");

                while (!cancellationToken.IsCancellationRequested)
                {
                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);

                    UserInteractionsHelper.WriteDebug($"Waiting for new messages from topic [{consumerStartRequest.TopicName}] (timeout {TimeoutSeconds}s)...");

                    var consumerResult = consumer.Consume(TimeSpan.FromSeconds(TimeoutSeconds));

                    if (consumerResult == null)
                    {
                        UserInteractionsHelper.WriteWarning("Nothing returned from broker.");
                        continue;
                    }

                    UserInteractionsHelper.WriteSuccess("===>>> Message received.");

                    ConsumerPrintServices.PrintConsumerResultData(consumerResult);

                    bool isKeyAvro = IsAvroMessage(consumerResult.Message.Key, out int? keySchemaId);
                    bool isValueAvro = IsAvroMessage(consumerResult.Message.Value, out int? valueSchemaId);

                    ConsumerPrintServices.PrintRawMessagePreview(consumerResult, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

                    if (usingSchemaRegistry && (isKeyAvro || isValueAvro))
                        ConsumerPrintServices.PrintAvroSchemas(schemaRegistry, keySchemaId, valueSchemaId);

                    // User options
                    ProcessUserOptions(consumer, consumerResult, out bool stopConsumer);

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

        private IConsumer<byte[], byte[]> CreateConsumer(ConsumerStartRequest consumeStartOptions)
        {
            var consumer = _consumerBuilder.BuildConsumer(consumeStartOptions);

            UserInteractionsHelper.WriteInformation("Consumer created...");

            return consumer;
        }

        private bool TryCreateSchemaRegistryClient(ConsumerStartRequest consumeStartOptions, out ISchemaRegistryClient schemaRegistryClient)
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

        private static void ConfirmAndCommitMessage(IConsumer<byte[], byte[]> consumer, ConsumeResult<byte[], byte[]> consumerResult)
        {
            if (UserInteractionsHelper.RequestYesNoResponse("Confirm COMMIT?") != "Y")
            {
                UserInteractionsHelper.WriteWarning("Commit aborted");
                return;
            }

            consumer.Commit(consumerResult);
            UserInteractionsHelper.WriteSuccess("Message commited.");
        }

        private static bool ProcessUserOptions(IConsumer<byte[], byte[]> consumer, ConsumeResult<byte[], byte[]> consumerResult, out bool stopConsumer)
        {
            stopConsumer = false;
            bool continueConsuming = false;

            while (!stopConsumer && !continueConsuming)
            {
                var userOption = RequestUserOption();

                switch (userOption)
                {
                    case 1: // Continue consumer
                        continueConsuming = true;
                        break;
                    case 2:
                        ConsumerPrintServices.PrintRawMessageKey(consumerResult.Message);
                        break;
                    case 3:
                        ConsumerPrintServices.PrintRawMessageValue(consumerResult.Message);
                        break;
                    case 4:
                        ConfirmAndCommitMessage(consumer, consumerResult);
                        break;
                    case 5:
                        ExportMessageService.ExportMessage(consumerResult.Message);
                        break;
                    case 6:
                        stopConsumer = true;
                        break;
                }
            }

            return stopConsumer;
        }

        private static int RequestUserOption()
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
    }
}
