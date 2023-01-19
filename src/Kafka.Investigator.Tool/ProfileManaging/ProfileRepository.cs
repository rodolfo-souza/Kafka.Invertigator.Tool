using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.ProfileManaging
{
    internal class ProfileRepository
    {
        private ProfileSet _profileSet = new();

        public ProfileRepository()
        {
            LoadProfilesFile();
        }

        #region Connection
        public ConnectionProfile? GetConnection(string connectionName)
        {
            return _profileSet.Connections.FirstOrDefault(p => p.ConnectionName.ToUpper() == connectionName.ToUpper());
        }

        public IReadOnlyList<ConnectionProfile> GetConnections()
        {
            return _profileSet.Connections.ToList();
        }

        public void AddOrReplaceConnection(ConnectionProfile connection)
        {
            var existingConnection = GetConnection(connection.ConnectionName);

            if (existingConnection == null)
                _profileSet.Connections.Add(connection);
            else
            {
                _profileSet.Connections.Remove(existingConnection);
                _profileSet.Connections.Add(connection);
            }

            SaveChanges();
        }

        public void DelConnection(ConnectionProfile connection)
        {
            var existingConnection = GetConnection(connection.ConnectionName);

            if (existingConnection == null)
                return;

            _profileSet.Connections.Remove(existingConnection);

            SaveChanges();
        }
        #endregion

        #region SchemaRegistry
        public SchemaRegistryProfile? GetSchemaRegistry(string schemaName)
        {
            return _profileSet.SchemaRegistries.FirstOrDefault(s => s.SchemaRegistryName.ToUpper() == schemaName.ToUpper());
        }

        public IReadOnlyList<SchemaRegistryProfile> GetSchemaRegistries()
        {
            return _profileSet.SchemaRegistries.ToList();
        }

        public void AddOrReplaceSchemaRegistry(SchemaRegistryProfile schemaRegistryProfile)
        {
            var existingSchema = GetSchemaRegistry(schemaRegistryProfile.SchemaRegistryName);

            if (existingSchema == null)
                _profileSet.SchemaRegistries.Add(schemaRegistryProfile);
            else
            {
                _profileSet.SchemaRegistries.Remove(existingSchema);
                _profileSet.SchemaRegistries.Add(schemaRegistryProfile);
            }

            SaveChanges();
        }

        public void DelSchemaRegistry(SchemaRegistryProfile schemaRegistryProfile)
        {
            var existingSchema = GetSchemaRegistry(schemaRegistryProfile.SchemaRegistryName);

            if (existingSchema == null)
                return;

            _profileSet.SchemaRegistries.Remove(existingSchema);

            SaveChanges();
        }

        #endregion

        #region Consumer

        public ConsumerProfile? GetConsumerProfile(string consumerName)
        {
            return _profileSet.ConsumerProfiles.FirstOrDefault(s => s.ConsumerName.ToUpper() == consumerName.ToUpper());
        }

        public IReadOnlyList<ConsumerProfile> GetConsumerProfiles()
        {
            return _profileSet.ConsumerProfiles.ToList();
        }

        public void AddOrReplaceConsumerProfile(ConsumerProfile consumerProfile)
        {
            var existingConsumer = GetConsumerProfile(consumerProfile.ConsumerName);

            if (existingConsumer == null)
                _profileSet.ConsumerProfiles.Add(consumerProfile);
            else
            {
                _profileSet.ConsumerProfiles.Remove(existingConsumer);
                _profileSet.ConsumerProfiles.Add(consumerProfile);
            }

            SaveChanges();
        }

        public void DelConsumerProfile(ConsumerProfile consumerProfile)
        {
            var existingConsumerProfile = GetSchemaRegistry(consumerProfile.ConsumerName);

            if (existingConsumerProfile == null)
                return;

            _profileSet.ConsumerProfiles.Remove(consumerProfile);

            SaveChanges();
        }

        #endregion

        private void SaveChanges()
        {
            var profilesFullPath = GetProfilesFilePath();

            var profilesJson = JsonSerializer.Serialize(_profileSet, GetJsonSerializerOptions());

            if (!Directory.Exists(profilesFullPath))
                Directory.CreateDirectory(Path.GetDirectoryName(profilesFullPath));

            File.WriteAllText(profilesFullPath, profilesJson);
        }

        private void LoadProfilesFile()
        {
            var profilesFullPath = GetProfilesFilePath();

            if (!File.Exists(profilesFullPath))
                return;

            var profilesJson = File.ReadAllText(profilesFullPath);

            try
            {
                _profileSet = JsonSerializer.Deserialize<ProfileSet>(profilesJson, GetJsonSerializerOptions());
            }
            catch (Exception)
            {
                Console.WriteLine($"The file [{Path.GetFileName(profilesFullPath)}] is in invalid format and will be deleted.");
                File.Delete(profilesFullPath);
                
                _profileSet = new();
            }
        }

        private static string GetProfilesFilePath()
        {
            var userPath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            return Path.Combine(userPath, ".kafkainvestigator", "profiles.json");
        }

        private JsonSerializerOptions GetJsonSerializerOptions()
        {
            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };

            options.Converters.Add(new JsonStringEnumConverter());

            return options;
        }
    }
}
