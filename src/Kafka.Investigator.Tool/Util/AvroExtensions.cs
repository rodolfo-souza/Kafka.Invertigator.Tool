using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Util
{
    internal static class AvroExtensions
    {
        public static bool IsAvro(this byte[] messagePart) => IsAvro(messagePart, out int? _);
        public static bool IsAvro(this byte[] messagePart, out int? schemaId)
        {
            // Constant used for AvroSerializer.
            // https://github.com/confluentinc/confluent-kafka-dotnet/blob/59e0243d8bf6e8456b4c9f853c926cf449e89ac8/src/Confluent.SchemaRegistry.Serdes.Avro/Constants.cs
            byte MagicByte = 0;

            schemaId = null;

            if (messagePart == null || messagePart.Length == 0)
                return false;

            using (var stream = new MemoryStream(messagePart))
            using (var reader = new BinaryReader(stream))
            {
                var firstByte = reader.ReadByte();

                if (firstByte != MagicByte)
                    return false;

                var schemaIdByte = reader.ReadInt32();

                schemaId = IPAddress.NetworkToHostOrder(schemaIdByte);

                return true;
            }
        }
    }
}
