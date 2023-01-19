using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConsumerProfileDelInteraction
    {
        private readonly ProfileRepository _profileRepository;

        public ConsumerProfileDelInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public void DelConsumerProfile(ConsumerProfileDelOptions options)
        {
            try
            {
                var existingConsumerProfile = _profileRepository.GetConsumerProfile(options.ConsumerProfileName);

                if (existingConsumerProfile == null)
                {
                    UserInteractionsHelper.WriteWarning($"Consumer profile [{options.ConsumerProfileName}] not found.");
                    return;
                }

                if (UserInteractionsHelper.RequestUserResponse($"Confirm EXCLUSION of profile [{options.ConsumerProfileName}]? Y/N", color: ConsoleColor.Yellow) != "Y")
                    return;

                _profileRepository.DelConsumerProfile(existingConsumerProfile);

                UserInteractionsHelper.WriteSuccess($"Profile [{options.ConsumerProfileName}] deleted.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error trying to delete a SchemaRegistry: " + ex.Message);
            }

        }
    }
}
