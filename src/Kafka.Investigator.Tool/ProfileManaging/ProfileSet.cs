namespace Kafka.Investigator.Tool.ProfileManaging
{
    internal class ProfileSet
    {
        public IList<ConnectionProfile> Connections { get; set; } = new List<ConnectionProfile>();
        public IList<SchemaRegistryProfile> SchemaRegistries { get; set; } = new List<SchemaRegistryProfile>();
        public IList<ConsumerProfile> ConsumerProfiles { get; set; } = new List<ConsumerProfile>();
    }
}
