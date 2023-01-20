using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-add", HelpText = "Add new consumer profile with topic name, group id etc. for easy run.")]
    internal class ConsumerProfileAddOptions : IRequest
    {
    }
}
