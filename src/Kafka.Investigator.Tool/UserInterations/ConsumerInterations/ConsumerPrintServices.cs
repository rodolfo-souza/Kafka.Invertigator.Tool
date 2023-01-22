using Confluent.Kafka;
using Confluent.SchemaRegistry;
using ConsoleTables;
using Kafka.Investigator.Tool.Util;
using System.Text;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerPrintServices
    {
        internal static void PrintConsumerResultData(ConsumeResult<byte[], byte[]> consumerResult)
        {
            var consoleTable = new ConsoleTable("Partition", "Offset", "Timestamp");

            consoleTable.AddRow(consumerResult.Partition.Value, consumerResult.Offset.Value, consumerResult.Message.Timestamp.UtcDateTime.ToLocalTime());

            consoleTable.WriteWithOptions(title: "Consume result", color: ConsoleColor.Blue, format: Format.Minimal);
        }

        internal static void PrintConsumerCurrentAssignment(IConsumer<byte[], byte[]> consumer)
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

            consoleTable.WriteWithOptions(title: "Current consumer assignment", format: Format.Minimal);
        }

        internal static void PrintRawMessagePreview(ConsumeResult<byte[], byte[]> consumerResult, bool isKeyAvro, int? keySchemaId, bool isValueAvro, int? valueSchemaId)
        {
            var rawKey = GetRawValue(consumerResult.Message.Key);
            var rawValue = GetRawValue(consumerResult.Message.Value);

            var rawMessageTable = new ConsoleTable("-", "Avro", "SchemaId", "Raw Preview (Avro values are unreadable)");

            rawMessageTable.AddRow("Key", isKeyAvro, keySchemaId, rawKey.Limit(170, " [more...]"));
            rawMessageTable.AddRow("Value", isValueAvro, valueSchemaId, rawValue.Limit(170, " [more...]"));

            rawMessageTable.WriteWithOptions(title: "Raw Message Preview", color: ConsoleColor.Blue, format: Format.Minimal);
        }

        internal static void PrintAvroSchemas(ISchemaRegistryClient schemaRegistry, int? keySchemaId, int? valueSchemaId)
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

        internal static void PrintRawMessageKey(Message<byte[], byte[]> message)
        {
            UserInteractionsHelper.WriteInformation("Raw Message Key");
            Console.WriteLine(GetRawValue(message.Key));
        }

        internal static void PrintRawMessageValue(Message<byte[], byte[]> message)
        {
            UserInteractionsHelper.WriteInformation("Raw Message Value");
            Console.WriteLine(GetRawValue(message.Value));
        }

        private static string GetRawValue(byte[] messagePart)
        {
            if (messagePart is null)
                return "<null>";

            return Encoding.UTF8.GetString(messagePart);
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
    }
}
