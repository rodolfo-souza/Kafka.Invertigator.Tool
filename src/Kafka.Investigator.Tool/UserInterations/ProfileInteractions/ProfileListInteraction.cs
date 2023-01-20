using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ProfileListInteraction : IRequestHandler<ConnectionListOptions>,
                                            IRequestHandler<SchemaRegistryListOptions>,
                                            IRequestHandler<ConsumerProfileListOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public ProfileListInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(ConnectionListOptions connectionListOptions, CancellationToken cancellationToken)
        {
            var connections = _profileRepository.GetConnections();

            ProfilePrintServices.PrintConnections(connections);

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(SchemaRegistryListOptions schemaRegistryListOptions, CancellationToken cancellationToken)
        {
            var schemaRegistries = _profileRepository.GetSchemaRegistries();

            ProfilePrintServices.PrintSchemaRegistries(schemaRegistries);

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConsumerProfileListOptions consumerProfileListOptions, CancellationToken cancellationToken)
        {
            var consumerProfiles = _profileRepository.GetConsumerProfiles();

            ProfilePrintServices.PrintConsumerProfiles(consumerProfiles);

            return Task.FromResult(Unit.Value);
        }
    }
}
