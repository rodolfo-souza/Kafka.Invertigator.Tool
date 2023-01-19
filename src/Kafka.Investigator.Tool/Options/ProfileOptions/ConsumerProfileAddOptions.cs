using CommandLine;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-add", HelpText = "Add new Consumer Profile with topic name, group id etc. for easy run.")]
    internal class ConsumerProfileAddOptions : INotification
    {
    }
}
