using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-list", HelpText = "List all consumer profiles configured for user.")]
    internal class ConsumerProfileListOptions : IRequest
    {
    }
}
