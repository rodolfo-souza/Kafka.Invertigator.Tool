using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ConsumerOptions
{
    [Verb("consumer-profile-start", HelpText = "Starts a consumer from consumer profile.")]
    internal class ConsumerProfileStartOptions : IRequest
    {
        [Option('p', "consumer-profile", Required = true, HelpText = "Consumer profile that contains all other parameters.")]
        public string? ConsumerProfileName { get; set; }
    }
}
