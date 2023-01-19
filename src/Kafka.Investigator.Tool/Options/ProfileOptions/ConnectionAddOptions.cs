using CommandLine;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("connection-add", HelpText = "Add or replace a connection.")]
    internal class ConnectionAddOptions : IRequest
    {
    }
}
