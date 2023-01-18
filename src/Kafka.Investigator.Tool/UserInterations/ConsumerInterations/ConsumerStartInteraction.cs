using Confluent.Kafka;
using Confluent.SchemaRegistry;
using ConsoleTables;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
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

        private const byte MagicByte = 0;

        public ConsumerStartInteraction(InvestigatorConsumerBuilder consumerBuilder, InvestigatorSchemaRegistryBuilder schemaRegistryBuilder)
        {
            _consumerBuilder = consumerBuilder;
            _schemaRegistryBuilder = schemaRegistryBuilder;
        }

        public void StartConsume(ConsumeStartOption consumeStartOptions, CancellationToken cancellationToken)
        {
            try
            {
                var consumer = CreateConsumer(consumeStartOptions);
                var usingSchemaRegistry = TryCreateSchemaRegistry(consumeStartOptions, out ISchemaRegistryClient schemaRegistry);

                UserInteractionsHelper.WriteDebug("Starting consumer...");

                while (!cancellationToken.IsCancellationRequested)
                {
                    UserInteractionsHelper.WriteDebug("Waiting for new messages...");
                    var consumerResult = consumer.Consume(cancellationToken);

                    if (consumerResult == null)
                        continue;

                    bool isKeyAvro = IsAvroMessage(consumerResult.Message.Key, out int? keySchemaId);
                    bool isValueAvro = IsAvroMessage(consumerResult.Message.Value, out int? valueSchemaId);

                    var consoleTable = new ConsoleTable("Partition", "Offset", "Is Key Avro?", "Key SchemaId", "Is Value Avro?", "Value SchemaId");

                    consoleTable.AddRow(consumerResult.Partition.Value, consumerResult.Offset.Value, isKeyAvro, keySchemaId, isValueAvro, valueSchemaId);

                    if (usingSchemaRegistry)
                    {
                        // TODO: Logar informações obtidas dos schemas.
                    }

                    UserInteractionsHelper.WriteWarning("Do you want to COMMIT message? Y/N");
                    if (Console.ReadLine().ToUpper() == "Y")
                    {
                        UserInteractionsHelper.WriteWarning("Confirm COMMIT? Y/N");
                        if (Console.ReadLine().ToUpper() == "Y")
                        {
                            consumer.Commit(consumerResult);
                            UserInteractionsHelper.WriteInformation("Message commited.");
                        }
                    }

                    UserInteractionsHelper.WriteInformation("Do you want to read next message? Y/N");
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

        private bool IsAvroMessage(byte[] messageBytes, out int? schemaId)
        {
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
            }

            return false;
        }
    }
}
