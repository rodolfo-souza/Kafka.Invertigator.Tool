using CommandLine;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-list", HelpText = "List all consumer profiles configured for user.")]
    internal class ConsumerProfileListOptions : IRequest
    {
    }
}
