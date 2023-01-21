using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [SubVerb(typeof(ConsumerProfileOptions), "del", HelpText = "Delete a consumer profile.")]
    internal class ConsumerProfileDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of consumer profile.")]
        public string ConsumerProfileName { get; set; }
    }
}
