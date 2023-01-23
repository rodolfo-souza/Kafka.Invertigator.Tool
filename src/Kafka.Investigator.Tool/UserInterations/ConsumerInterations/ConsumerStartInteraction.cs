using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Util;

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
                using var consumer = CreateConsumer(consumerStartRequest);

                var usingSchemaRegistry = TryCreateSchemaRegistryClient(consumerStartRequest, out ISchemaRegistryClient schemaRegistryClient);
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

                    bool isKeyAvro = consumerResult.Message.Key.IsAvro(out int ? keySchemaId);
                    bool isValueAvro = consumerResult.Message.Value.IsAvro(out int? valueSchemaId);

                    ConsumerPrintServices.PrintRawMessagePreview(consumerResult, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

                    if (usingSchemaRegistry && (isKeyAvro || isValueAvro))
                        ConsumerPrintServices.PrintAvroSchemas(schemaRegistryClient, keySchemaId, valueSchemaId);

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
                    case "N":
                        continueConsuming = true;
                        break;
                    case "K":
                        ConsumerPrintServices.PrintRawMessageKey(consumerResult.Message);
                        break;
                    case "M":
                        ConsumerPrintServices.PrintRawMessageValue(consumerResult.Message);
                        break;
                    case "C":
                        ConfirmAndCommitMessage(consumer, consumerResult);
                        break;
                    case "S":
                        ExportMessageService.ExportMessage(consumerResult.Message);
                        break;
                    case "H":
                        ConsumerPrintServices.PrintMessageHeaders(consumerResult.Message);
                        break;
                    case "E":
                        stopConsumer = true;
                        break;
                }
            }

            return stopConsumer;
        }

        private static string RequestUserOption()
        {
            var validOptions = new[] { "N", "K", "M", "H", "C", "S", "E" };

            while(true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> MESSAGE OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("N - Continue consuming (without commit)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("K - Print Key (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("M - Print Value (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("H - Print Message Headers", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("C - Commit message offset", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("S - Save message (export as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("E - Finish consumer", ConsoleColor.Yellow);

                var userOption = UserInteractionsHelper.RequestUserResponseKey("Select an option: ", ConsoleColor.Yellow, responseToUpper: true);

                Console.WriteLine();

                if (validOptions.Contains(userOption))
                    return userOption;

                UserInteractionsHelper.WriteError($"Invalid option.");
            }
            
        }
    }
}
