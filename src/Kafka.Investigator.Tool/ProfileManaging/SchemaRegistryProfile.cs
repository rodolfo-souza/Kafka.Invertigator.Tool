namespace Kafka.Investigator.Tool.ProfileManaging
{
    internal class SchemaRegistryProfile
    {
        public SchemaRegistryProfile(string schemaRegistryName, bool @default, string url, string userName, string password)
        {
            SchemaRegistryName = schemaRegistryName;
            Default = @default;
            Url = url;
            UserName = userName;
            Password = password;
        }

        public string SchemaRegistryName { get; set; }
        public bool Default { get; set; }
        public string Url { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }

        public void Validate()
        {
            if (string.IsNullOrEmpty(SchemaRegistryName))
                throw new Exception("SchemaRegistry Name is required.");

            if (string.IsNullOrEmpty(Url))
                throw new Exception("SchemaRegistry Url is required.");
        }
    }
}
