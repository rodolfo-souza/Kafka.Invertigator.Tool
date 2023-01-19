using CommandLine;
using CommandLine.Text;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("connection-list", HelpText = "List all connections configured for user.")]
    internal class ConnectionListOptions : INotification
    {
    }
}
