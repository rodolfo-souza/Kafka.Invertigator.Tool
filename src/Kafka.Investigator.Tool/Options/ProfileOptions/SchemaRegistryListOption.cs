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
    [Verb("schema-registry-list", HelpText = "List all schema registries configured for user.")]
    internal class SchemaRegistryListOption : INotification
    {
    }
}
