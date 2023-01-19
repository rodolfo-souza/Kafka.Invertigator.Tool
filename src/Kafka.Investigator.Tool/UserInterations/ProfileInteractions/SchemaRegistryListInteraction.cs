using ConsoleTables;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.Util;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class SchemaRegistryListInteraction
    {
        private readonly ProfileRepository _profileRepository;

        public SchemaRegistryListInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public void ListSchemaRegistries()
        {
            var schemaRegistries = _profileRepository.GetSchemaRegistries();

            var consoleTable = new ConsoleTable("Schema Name", "Default", "Url", "Username");

            foreach (var s in schemaRegistries)
                consoleTable.AddRow(s.SchemaRegistryName, s.Default == true ? "***" : "", s.Url, s.UserName);

            consoleTable.WriteWithOptions();
        }
    }
}
