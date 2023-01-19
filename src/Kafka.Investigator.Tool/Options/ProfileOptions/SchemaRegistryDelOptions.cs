using CommandLine;
using CommandLine.Text;
using Kafka.Investigator.Tool.UserInterations;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("schema-registry-del", HelpText = "Delete a Schema Registry.")]
    internal class SchemaRegistryDelOptions : INotification
    {
        [Option('n', "name", Required = true, HelpText = "Name of Schema Registry.")]
        public string SchemaRegistryName { get; set; }
    }
}
