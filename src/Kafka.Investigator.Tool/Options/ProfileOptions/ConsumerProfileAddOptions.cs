using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(ConsumerProfileOptions), "add", HelpText = "Add new consumer profile with topic name, group id etc. for easy run.")]
    internal class ConsumerProfileAddOptions : IRequest
    {
    }
}
