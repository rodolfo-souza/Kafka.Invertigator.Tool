using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(ConsumerProfileOptions), "list", HelpText = "List all consumer profiles configured for user.")]
    internal class ConsumerProfileListOptions : IRequest
    {
    }
}
