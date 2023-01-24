using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(SchemaRegistryOptions), "test", isDefault: false, HelpText = "Test a schema registry (check credentials).")]
    internal class SchemaRegistryTestOptions : IRequest
    {
        [Option('s', "schema-registry", Required = true, HelpText = "Schema Registry name that will be tested.")]
        public string SchemaRegistryName { get; set; }

        [Option('l', "list-subjects", Required = false, HelpText = "Lists all subjects found in the schema registry.")]
        public bool ListSubjects { get; set; }

        [Option('i', "schema-id", Required = false, HelpText = "Get a specific schema registry by id.")]
        public int? SchemaId { get; set; }
    }
}