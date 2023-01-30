using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Util;
using Polly;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerStartInteraction
    {
        private readonly InvestigatorConsumerBuilder _consumerBuilder;
        private readonly InvestigatorSchemaRegistryBuilder _schemaRegistryBuilder;

        private ISchemaRegistryClient _schemaRegistryClient = null;
        private ConsumerStartRequest _consumerStartRequest = null;

        private const int TimeoutSeconds = 3;

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

                while (nextAction != NagigationAction.StopConsume && !cancellationToken.IsCancellationRequested)
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

                    consumer.StoreOffset(consumerResult);

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

        private NagigationAction InteractWithUser(Menu startMenu, IConsumer<byte[], byte[]> consumer, bool beforeStart, ConsumeResult<byte[], byte[]>? consumeResult)
        {
            NagigationAction? navigationAction;

            try
            {
                switch (startMenu)
                {
                    case Menu.ConsumerOptions:
                        var consumerOption = RequestConsumerOptions(consumeResult != null, beforeStart);

                        navigationAction = ContainsNavigationAction(consumerOption);

                        if (navigationAction == NagigationAction.MessageMenu)
                            return InteractWithUser(Menu.MessageOptions, consumer, beforeStart, consumeResult);

                        if (navigationAction != null)
                            return navigationAction.Value;

                        ProcessConsumerOption(consumerOption, consumer);
                        break;

                    case Menu.MessageOptions:
                        var messageOption = RequestMessageOptions(consumeResult);

                        navigationAction = ContainsNavigationAction(messageOption);

                        if (navigationAction == NagigationAction.ConsumerMenu)
                            return InteractWithUser(Menu.ConsumerOptions, consumer, beforeStart, consumeResult);

                        if (navigationAction != null)
                            return navigationAction.Value;

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

        private static NagigationAction? ContainsNavigationAction(string userOption)
        {
            switch (userOption)
            {
                case "Q":
                    return NagigationAction.StopConsume;
                case "M":
                    return NagigationAction.MessageMenu;
                case "B":
                    return NagigationAction.ConsumerMenu;
                case "N":
                    return NagigationAction.ContineConsume;
                default:
                    return null;
            }
        }

        private string RequestConsumerOptions(bool offerBackMessageOptions, bool beforeFirstConsume)
        {
            List<string> validOptions = new() { "N", "A", "P", "Q" };

            if (!beforeFirstConsume)
                validOptions.AddRange(new[] { "C", "F", "E", "T" });

            if (offerBackMessageOptions)
                validOptions.Add("M");

            string schemaRegistryObs = _schemaRegistryClient is null ? $"(disabled: no schema registry selected)" : $"(using schema registry [{_consumerStartRequest.SchemaRegistryName}])";

            while (true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor("===>>> CONSUMER OPTIONS:", ConsoleColor.DarkYellow);
                UserInteractionsHelper.WriteWithColor("[N] - Next Message (consume with current assignment)", ConsoleColor.DarkYellow);
                UserInteractionsHelper.WriteWithColor("[A] - Print current Assignment", ConsoleColor.DarkYellow);
                UserInteractionsHelper.WriteWithColor("[P] - Print Topic Partitions ", ConsoleColor.DarkYellow);

                if (!beforeFirstConsume)
                {
                    UserInteractionsHelper.WriteWithColor("[C] - Commit Current Assignment (ATENTION: print current assignment to see all partitions and offsets)", ConsoleColor.DarkYellow);
                    UserInteractionsHelper.WriteWithColor("[F] - Force All Partitions Assignment", ConsoleColor.DarkYellow);
                    UserInteractionsHelper.WriteWithColor("[E] - Force Earliest Consume (also force all partitions assignment)", ConsoleColor.DarkYellow);
                    UserInteractionsHelper.WriteWithColor("[T] - Force Consume By Time (also force all partitions assignment)", ConsoleColor.DarkYellow);
                }

                if (offerBackMessageOptions)
                {
                    UserInteractionsHelper.WriteWithColor("[M] - Back to Message Options", ConsoleColor.DarkYellow);
                }

                UserInteractionsHelper.WriteWithColor("[Q] - Quit (stop consumer)", ConsoleColor.DarkYellow);

                var userOption = UserInteractionsHelper.RequestUserResponseKey("Select an option: ", ConsoleColor.DarkYellow, responseToUpper: true);

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
                case "C":
                    ConfirmAndCommitAssignment(consumer);
                    break;
                case "F":
                    consumer.AssignAllPartitions();
                    UserInteractionsHelper.WriteSuccess("Force Assignment OK. Request Next message to refresh Offsets.");
                    ConsumerPrintServices.PrintConsumerCurrentAssignment(consumer);
                    break;
                case "E":
                    consumer.ForceConsumeEarliest();
                    UserInteractionsHelper.WriteSuccess("Force Earliest consume OK. Request Next message to refresh Offsets. ATENTION: This action was not commited in Broker. If you want to commit the Earliest offset, use COMMIT option.");
                    break;
                case "T":
                    var datetime = UserInteractionsHelper.RequestInput<DateTime>("Datetime (yyyy-MM-dd HH:mm:ss)");
                    consumer.ForceConsumeByTime(datetime);
                    UserInteractionsHelper.WriteSuccess($"Force consume from {datetime:yyyy-MM-dd HH:mm:ss} OK. Request Next message to refresh Offsets. ATENTION: This action was not commited in Broker. If you want to commit the Earliest offset, use COMMIT option.");
                    break;
                default:
                    throw new NotImplementedException("Consumer option not implemented: " + userOption);
            }
        }

        private string RequestMessageOptions(ConsumeResult<byte[], byte[]> consumeResult)
        {
            var validOptions = new[] { "N", "K", "V", "M", "A", "R", "H", "C", "S", "O", "B", "Q" };

            string schemaRegistryObs = _schemaRegistryClient is null ? $"(disabled: no schema registry selected)" : $"(using schema registry [{_consumerStartRequest.SchemaRegistryName}])";

            while (true)
            {
                UserInteractionsHelper.WriteEmptyLine();
                UserInteractionsHelper.WriteWithColor($"===>>> MESSAGE OPTIONS:", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[N] - Next Message (consume with current assignment)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[K] - Print Key (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[V] - Print Value (full)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[H] - Print Headers", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[M] - Message Preview (reprint)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[A] - Print current assignment (partitions assigned for this consumer)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[R] - Print Message Schemas " + schemaRegistryObs, ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[C] - Commit message (partition {consumeResult.Partition.Value}, offset {consumeResult.Offset.Value})", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[S] - Save message (export as file)", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[B] - Back to Consumer Options", ConsoleColor.Yellow);
                UserInteractionsHelper.WriteWithColor($"[Q] - Quit (stop consumer)", ConsoleColor.Yellow);

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

        private static void ConfirmAndCommitAssignment(IConsumer<byte[], byte[]> consumer)
        {
            try
            {
                if (UserInteractionsHelper.RequestYesNoResponse("Confirm COMMIT? ATENTION: this will commit all partitions and offsets for current assignment.") != "Y")
                {
                    UserInteractionsHelper.WriteWarning("Commit aborted");
                    return;
                }

                var retryPolicy = Policy.Handle<Exception>(e => e.Message.Contains("Group rebalance in progress") ||
                                                                e.Message.Contains("Unknown member"))
                                        .WaitAndRetry(retryCount: 3,
                                                      sleepDurationProvider: (attempt) => TimeSpan.FromSeconds(attempt),
                                                      onRetry: (ex, retryCount) => UserInteractionsHelper.WriteError($"{ex.Message} - retrying..."));

                retryPolicy.Execute(() => consumer.Commit());

                UserInteractionsHelper.WriteSuccess("Current assignemt commited.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
                UserInteractionsHelper.WriteError("*** This might be a bug from Confluent.Kafka library. Try restart Kafka.Investigator and retry operation.");
            }
        }

        private static void ConfirmAndCommitMessage(IConsumer<byte[], byte[]> consumer, ConsumeResult<byte[], byte[]> consumerResult)
        {
            try
            {
                if (UserInteractionsHelper.RequestYesNoResponse($"Confirm COMMIT? (only partitoin {consumerResult.Partition.Value} offset {consumerResult.Offset})") != "Y")
                {
                    UserInteractionsHelper.WriteWarning("Commit aborted");
                    return;
                }

                consumer.Commit(consumerResult);

                UserInteractionsHelper.WriteSuccess("Message commited.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
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

    internal enum Menu
    {
        ConsumerOptions,
        MessageOptions
    }

    internal enum NagigationAction
    {
        ContineConsume,
        StopConsume,
        ConsumerMenu,
        MessageMenu
    }
}
