using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Util
{
    internal static class ConsumerExtensions
    {
        private static TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Query (in broker) the current watermark of all partitions for the first topic subscription.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="consumer"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static Dictionary<TopicPartition, WatermarkOffsets> QueryWatermarkOffsetsAllPartitions<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
        {
            var topicName = consumer.Subscription.FirstOrDefault();

            if (topicName == null)
                throw new Exception("Consumer don't have a topic subscription. Subscribe a topic first.");

            var allPartitionsWatermarks = new Dictionary<TopicPartition, WatermarkOffsets>();

            for (int i = 0; i < 100; i++)
            {
                var topicPartitionToCheck = new TopicPartition(topicName, i);
                TryGetPartitionWartermarkOffset(consumer, topicPartitionToCheck, out WatermarkOffsets? partitionInfo);

                if (partitionInfo == null)
                    break;

                allPartitionsWatermarks.Add(topicPartitionToCheck, partitionInfo);
            }

            return allPartitionsWatermarks;
        }

        public static bool TryGetPartitionWartermarkOffset<TKey, TValue>(this IConsumer<TKey, TValue> consumer, TopicPartition partition, out WatermarkOffsets? watermarkOffsets)
        {
            watermarkOffsets = null;

            try
            {
                watermarkOffsets = consumer.QueryWatermarkOffsets(partition, DefaultTimeout);
                return true;
            }
            catch (Exception)
            {
            }

            return false;
        }

        public static List<TopicPartitionOffset> GetOffsetsByTime<TKey, TValue>(this IConsumer<TKey, TValue> consumer, DateTimeOffset dateTimeOffset)
        {
            IEnumerable<TopicPartition> topicPartitions = consumer.QueryWatermarkOffsetsAllPartitions()?.Keys;

            if (!topicPartitions.Any())
                return Enumerable.Empty<TopicPartitionOffset>().ToList();

            var query = new List<TopicPartitionTimestamp>();

            foreach (var item in topicPartitions)
                query.Add(new TopicPartitionTimestamp(item, new Timestamp(dateTimeOffset)));

            return consumer.OffsetsForTimes(query, DefaultTimeout);
        }

        public static void AssignAllPartitions<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
        {
            try
            {
                var allPartitions = consumer.QueryWatermarkOffsetsAllPartitions()?.Keys;

                if (allPartitions == null || allPartitions.Count == 0)
                    throw new Exception($"There's no partition to assign in topic [{consumer.Subscription.FirstOrDefault()}]. Check if the topic name is correct.");

                consumer.Assign(allPartitions);
            }
            catch (Exception ex)
            {
                throw new Exception("Error trying to assign all partitions: " + ex.Message);
            }
        }

        public static void ForceConsumeEarliest<TKey, TValue>(this IConsumer<TKey, TValue> consumer)
        {
            try
            {
                var allPartitions = consumer.QueryWatermarkOffsetsAllPartitions();

                if (allPartitions == null || allPartitions.Count == 0)
                    throw new Exception($"There's no partition to assign in topic [{consumer.Subscription.FirstOrDefault()}]. Check if the topic name is correct.");

                var earliestPartitionOffsets = allPartitions.Select(x => new TopicPartitionOffset(x.Key, new Offset(x.Value.Low)));

                
                consumer.Assign(earliestPartitionOffsets);

                foreach (var earliestPartitionOffset in earliestPartitionOffsets)
                {
                    consumer.StoreOffset(earliestPartitionOffset);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error trying to force consume earliest: " + ex.Message);
            }
        }

        public static void ForceConsumeByTime<TKey, TValue>(this IConsumer<TKey, TValue> consumer, DateTime dateTime)
        {
            try
            {
                var offsetsByTime = GetOffsetsByTime(consumer, dateTime);

                consumer.Assign(offsetsByTime);

                foreach (var offsetByTime in offsetsByTime)
                {
                    consumer.StoreOffset(offsetByTime);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error trying to force consume by time: " + ex.Message);
            }
        }
    }
}
