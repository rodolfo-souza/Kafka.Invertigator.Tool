using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.OptionsHandlers
{
    internal class ConsumerStartOptionsHandler : INotificationHandler<ConsumeStartOption>
    {
        private readonly ProfileRepository _profileRepository;
        private readonly ConsumerStartInteraction _consumerStartInteraction;

        public ConsumerStartOptionsHandler(ProfileRepository profileRepository, ConsumerStartInteraction consumerStartInteraction)
        {
            _profileRepository = profileRepository;
            _consumerStartInteraction = consumerStartInteraction;
        }

        public Task Handle(ConsumeStartOption consumerOptions, CancellationToken cancellationToken)
        {
            try
            {
                ConsumerStartRequest startRequest;

                if (!string.IsNullOrEmpty(consumerOptions.ConsumerProfileName))
                    startRequest = BuildRequestFromConsumerProfile(consumerOptions.ConsumerProfileName);
                else
                    startRequest = new ConsumerStartRequest(consumerOptions);

                _consumerStartInteraction.StartConsume(startRequest, cancellationToken);
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError(ex.Message);
            }

            return Task.CompletedTask;

        }

        private ConsumerStartRequest BuildRequestFromConsumerProfile(string consumerProfileName)
        {
            var consumerProfile = _profileRepository.GetConsumerProfile(consumerProfileName);

            if (consumerProfile == null)
                throw new Exception($"The informed consumer profile [{consumerProfileName}] was not found.");

            UserInteractionsHelper.WriteWarning($"Using parameters from consumer profile [{consumerProfileName}].");

            return new ConsumerStartRequest(consumerProfile);
        }
    }
}
