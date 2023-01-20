using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ConsumerInterations
{
    internal class ConsumerStartOptionsHandler : IRequestHandler<ConsumerStartOptions>,
                                                 IRequestHandler<ConsumerProfileStartOptions>
    {
        private readonly ProfileRepository _profileRepository;
        private readonly ConsumerStartInteraction _consumerStartInteraction;

        public ConsumerStartOptionsHandler(ProfileRepository profileRepository, ConsumerStartInteraction consumerStartInteraction)
        {
            _profileRepository = profileRepository;
            _consumerStartInteraction = consumerStartInteraction;
        }

        public Task<Unit> Handle(ConsumerStartOptions consumerOptions, CancellationToken cancellationToken)
        {
            try
            {
                UserInteractionsHelper.WriteWarning($"Starting consumer without consumer profile.");

                var startRequest = new ConsumerStartRequest(consumerOptions);
                _consumerStartInteraction.StartConsume(startRequest, cancellationToken);
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }

            return Task.FromResult(Unit.Value);
        }

        public Task<Unit> Handle(ConsumerProfileStartOptions consumerOptions, CancellationToken cancellationToken)
        {
            try
            {
                UserInteractionsHelper.WriteWarning($"Starting consumer from consumer profile [{consumerOptions.ConsumerProfileName}].");

                var startRequest = BuildRequestFromConsumerProfile(consumerOptions.ConsumerProfileName);

                _consumerStartInteraction.StartConsume(startRequest, cancellationToken);
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }

            return Task.FromResult(Unit.Value);
        }

        private ConsumerStartRequest BuildRequestFromConsumerProfile(string consumerProfileName)
        {
            var consumerProfile = _profileRepository.GetConsumerProfile(consumerProfileName);

            if (consumerProfile == null)
                throw new Exception($"The informed consumer profile [{consumerProfileName}] was not found.");

            return new ConsumerStartRequest(consumerProfile);
        }
    }
}
