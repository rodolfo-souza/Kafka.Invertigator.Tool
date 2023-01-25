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

                var nextAction = InteractWithUser(Menu.ConsumerOptions, consumer, beforeStart: true, consumeResult: null);

                while (nextAction != ConsumerAction.StopConsume && !cancellationToken.IsCancellationRequested)
                {
                    UserInteractionsHelper.WriteDebug($"Waiting for new messages from topic [{consumerStartRequest.TopicName}] (timeout {TimeoutSeconds}s)...");

                    var consumerResult = consumer.Consume(TimeSpan.FromSeconds(TimeoutSeconds));

                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);

                    if (consumerResult == null)
                    {
                        UserInteractionsHelper.WriteWarning("Nothing returned from broker.");
                        nextAction = InteractWithUser(Menu.ConsumerOptions, consumer, beforeStart: false, consumeResult: null);
                        continue;
                    }

                    ConsumerPrintServices.PrintConsumerResultData(consumerResult);

                    ConsumerPrintServices.PrintRawMessagePreview(consumerResult);

                    // User options
                    nextAction = InteractWithUser(Menu.MessageOptions, consumer, beforeStart: false, consumeResult: consumerResult);
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

        private ConsumerAction InteractWithUser(Menu startMenu, IConsumer<byte[], byte[]> consumer, bool beforeStart, ConsumeResult<byte[], byte[]>? consumeResult)
        {
            ConsumerAction? consumerAction;

            try
            {
                switch (startMenu)
                {
                    case Menu.ConsumerOptions:
                        var consumerOption = RequestConsumerOptions(consumeResult != null, beforeStart);

                        consumerAction = TranslateConsumerAction(consumerOption);

                        if (consumerAction == ConsumerAction.MessageMenu)
                            InteractWithUser(Menu.MessageOptions, consumer, beforeStart, consumeResult);

                        if (consumerAction != null)
                            return consumerAction.Value;

                        ProcessConsumerOption(consumerOption, consumer);
                        break;

                    case Menu.MessageOptions:
                        var messageOption = RequestMessageOptions();

                        consumerAction = TranslateConsumerAction(messageOption);

                        if (consumerAction == ConsumerAction.ConsumerMenu)
                            InteractWithUser(Menu.ConsumerOptions, consumer, beforeStart, consumeResult);

                        if (consumerAction != null)
                            return consumerAction.Value;

                        ProcessMessageOption(messageOption, consumer, consumeResult);

                        break;
                    default:
                        break;
                }
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }

            // Show menu again.
            return InteractWithUser(startMenu, consumer, beforeStart, consumeResult);
        }

        private static ConsumerAction? TranslateConsumerAction(string userOption)
        {
            switch (userOption)
            {
                case "Q":
                    return ConsumerAction.StopConsume;
                case "M":
                    return ConsumerAction.MessageMenu;
                case "B":
                    return ConsumerAction.ConsumerMenu;
                case "N":
                    return ConsumerAction.ContineConsume;
                default:
                    return null;
            }
        }

        private string RequestConsumerOptions(bool offerBackMessageOptions, bool beforeFirstConsume)
        {
            List<string> validOptions = new() { "N", "A", "P", "Q" };

            if (!beforeFirstConsume)
                validOptions.AddRange(new[] { "F", "E" });

            if (offerBackMessageOptions)
                validOptions.Add("M");

            string schemaRegistryObs = _schemaRegistryClient is null ? $"(disabled: no schema registry selected)" : $"(using schema registry [{_consumerStartRequest.SchemaRegistryName}])";

            while (true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> CONSUMER OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[N] - Next Message (consume with current assignment)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[A] - Print current Assignment", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[P] - Print Topic Partitions ", ConsoleColor.Yellow);

                if (!beforeFirstConsume)
                {
                    UserInteractionsHelper.WriteWithColor("[F] - Force All Partitions Assignment", ConsoleColor.Yellow);
                    UserInteractionsHelper.WriteWithColor("[E] - Force Earliest Consume (also force all partitions assignment)", ConsoleColor.Yellow);
                }

                if (offerBackMessageOptions)
                {
                    UserInteractionsHelper.WriteWithColor("[M] - Back to Message Options", ConsoleColor.Yellow);
                }

                UserInteractionsHelper.WriteWithColor("[Q] - Quit (stop consumer)", ConsoleColor.Yellow);

                var userOption = UserInteractionsHelper.RequestUserResponseKey("Select an option: ", ConsoleColor.Yellow, responseToUpper: true);

                Console.WriteLine();

                if (validOptions.Contains(userOption))
                    return userOption;

                UserInteractionsHelper.WriteError($"Invalid option.");
            }
        }

        private static void ProcessConsumerOption(string userOption, IConsumer<byte[], byte[]> consumer)
        {
            switch (userOption)
            {
                case "A":
                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);
                    break;
                case "P":
                    ConsumerPrintServices.PrintTopicPartitions(consumer);
                    break;
                case "F":
                    consumer.AssignAllPartitions();
                    UserInteractionsHelper.WriteSuccess("Force Assignment OK. Request Next message to refresh Offsets.");
                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);
                    break;
                case "E":
                    consumer.ForceConsumeEarliest();
                    UserInteractionsHelper.WriteSuccess("Force Earliest consume OK. Request Next message to refresh Offsets.");
                    break;
                default:
                    throw new NotImplementedException("Consumer option not implemented: " + userOption);
            }
        }

        private string RequestMessageOptions()
        {
            var validOptions = new[] { "N", "K", "V", "M", "A", "R", "H", "C", "S", "O", "B", "Q" };

            string schemaRegistryObs = _schemaRegistryClient is null ? $"(disabled: no schema registry selected)" : $"(using schema registry [{_consumerStartRequest.SchemaRegistryName}])";

            while (true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> MESSAGE OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[N] - Next Message (consume with current assignment)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[K] - Print Key (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[V] - Print Value (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[H] - Print Headers", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[M] - Message Preview (reprint)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[A] - Print current assignment (partitions assigned for this consumer)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[R] - Print Message Schemas " + schemaRegistryObs, ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[C] - Commit message offset", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[S] - Save message (export as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[B] - Back to Consumer Options", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor("[Q] - Quit (stop consumer)", ConsoleColor.Yellow);

                var userOption = UserInteractionsHelper.RequestUserResponseKey("Select an option: ", ConsoleColor.Yellow, responseToUpper: true);

                Console.WriteLine();

                if (validOptions.Contains(userOption))
                    return userOption;

                UserInteractionsHelper.WriteError($"Invalid option.");
            }
        }

        private void ProcessMessageOption(string userOption, IConsumer<byte[], byte[]> consumer, ConsumeResult<byte[], byte[]>? consumerResult)
        {
            switch (userOption)
            {
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
                case "A":
                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);
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

    internal enum Menu
    {
        ConsumerOptions,
        MessageOptions
    }

    internal enum ConsumerAction
    {
        ContineConsume,
        StopConsume,
        ConsumerMenu,
        MessageMenu
    }
}
