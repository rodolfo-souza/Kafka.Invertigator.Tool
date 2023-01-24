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

        private ISchemaRegistryClient _schemaRegistryClient = null;
        private ConsumerStartRequest _consumerStartRequest = null;

        private const int TimeoutSeconds = 10;

        public ConsumerStartInteraction(InvestigatorConsumerBuilder consumerBuilder, InvestigatorSchemaRegistryBuilder schemaRegistryBuilder)
        {
            _consumerBuilder = consumerBuilder;
            _schemaRegistryBuilder = schemaRegistryBuilder;
        }

        public void StartConsume(ConsumerStartRequest consumerStartRequest, CancellationToken cancellationToken)
        {
            _consumerStartRequest = consumerStartRequest;
            try
            {
                using var consumer = _consumerBuilder.BuildConsumer(consumerStartRequest, printConsumerParameters: true);

                var usingSchemaRegistry = TryCreateSchemaRegistryClient(consumerStartRequest, out _schemaRegistryClient);
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

                    ConsumerPrintServices.PrintConsumerResultData(consumerResult);

                    ConsumerPrintServices.PrintRawMessagePreview(consumerResult);

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
                schemaRegistryClient = _schemaRegistryBuilder.BuildSchemaRegistryClient(consumeStartOptions.SchemaRegistryName, printSchemaRegistryParameters: true);
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

        private bool ProcessUserOptions(IConsumer<byte[], byte[]> consumer, ConsumeResult<byte[], byte[]> consumerResult, out bool stopConsumer)
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
                    case "V":
                        ConsumerPrintServices.PrintRawMessageValue(consumerResult.Message);
                        break;
                    case "M":
                        ConsumerPrintServices.PrintConsumerResultData(consumerResult);
                        ConsumerPrintServices.PrintRawMessagePreview(consumerResult);
                        break;
                    case "R":
                        PrintAvroSchemas(consumerResult.Message);
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
                    case "Q":
                        stopConsumer = true;
                        break;
                }
            }

            return stopConsumer;
        }

        private string RequestUserOption()
        {
            var validOptions = new[] { "N", "K", "V", "M", "R" ,"H", "C", "S", "Q" };

            string schemaRegistryObs = _schemaRegistryClient is null ? $"(disabled: no schema registry selected)" : $"(using schema registry [{_consumerStartRequest.SchemaRegistryName}])";

            while (true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> MESSAGE OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("N - Next Message", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("K - Print Key (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("V - Print Value (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("H - Print Headers", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("M - Message Preview (reprint)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("R - Print Message Schemas " + schemaRegistryObs, ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("C - Commit message offset", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("S - Save message (export as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("Q - Quit (stop consumer)", ConsoleColor.Yellow);

                var userOption = UserInteractionsHelper.RequestUserResponseKey("Select an option: ", ConsoleColor.Yellow, responseToUpper: true);

                Console.WriteLine();

                if (validOptions.Contains(userOption))
                    return userOption;

                UserInteractionsHelper.WriteError($"Invalid option.");
            }
            
        }

        private void PrintAvroSchemas(Message<byte[], byte[]> message)
        {
            if (_schemaRegistryClient == null)
            {
                UserInteractionsHelper.WriteError("There's no Schema Registry configured for consumer. Try restart consumer with option --schema-registry <SCHEMA_NAME>.");
                return;
            }

            bool isKeyAvro = message.Key.IsAvro(out int? keySchemaId);
            bool isValueAvro = message.Value.IsAvro(out int? valueSchemaId);

            if (!isKeyAvro && !isValueAvro)
            {
                UserInteractionsHelper.WriteError("Both Key and Value do not contains SchemaId.");
                return;
            }

            ConsumerPrintServices.PrintAvroSchemas(_schemaRegistryClient, keySchemaId, valueSchemaId);
        }
    }
}
