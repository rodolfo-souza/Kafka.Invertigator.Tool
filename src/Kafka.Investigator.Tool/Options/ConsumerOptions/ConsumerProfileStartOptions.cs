using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ConsumerOptions
{
    [SubVerb(typeof(ConsumerOptions), "start-profile", HelpText = "Starts a consumer from consumer profile.")]
    internal class ConsumerProfileStartOptions : IRequest
    {
        [Option('p', "consumer-profile", Required = true, HelpText = "Consumer profile that contains all other parameters.")]
        public string ConsumerProfileName { get; set; }
    }
}
