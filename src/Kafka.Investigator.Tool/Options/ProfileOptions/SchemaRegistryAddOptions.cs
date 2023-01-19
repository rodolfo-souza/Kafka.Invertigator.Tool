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
    [Verb("schema-registry-add", HelpText = "Add or replace a SchemaRegistry configuration for BasicAuthentication.")]
    internal class SchemaRegistryAddOptions : INotification
    {
    }
}
