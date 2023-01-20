using CommandLine;
using MediatR;

namespace Kafka.Investigator.Tool.Options.ProfileOptions
{
    [Verb("consumer-profile-del", HelpText = "Delete a consumer profile.")]
    internal class ConsumerProfileDelOptions : IRequest
    {
        [Option('n', "name", Required = true, HelpText = "Name of consumer profile.")]
        public string ConsumerProfileName { get; set; }
    }
}
