using CommandLine;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-del", HelpText = "Delete a consumer profile.")]
    internal class ConsumerProfileDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of consumer profile.")]
        public string ConsumerProfileName { get; set; }
    }
}
