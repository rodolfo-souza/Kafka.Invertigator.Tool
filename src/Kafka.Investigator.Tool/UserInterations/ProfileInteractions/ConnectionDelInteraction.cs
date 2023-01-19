using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConnectionDelInteraction
    {
        private readonly ProfileRepository _profileRepository;

        public ConnectionDelInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public void DelConnection(ConnectionDelOptions profileDelOptions)
        {
            try
            {
                var existingConnection = _profileRepository.GetConnection(profileDelOptions.ConnectionName);

                if (existingConnection == null)
                {
                    UserInteractionsHelper.WriteWarning($"Connection [{profileDelOptions.ConnectionName}] not found.");
                    return;
                }

                UserInteractionsHelper.WriteWarning($"Confirm EXCLUSION of connection [{profileDelOptions.ConnectionName}]? Y/N");
                if (Console.ReadLine().ToUpper() != "Y")
                    return;

                _profileRepository.DelConnection(existingConnection);

                UserInteractionsHelper.WriteSuccess($"Connection [{profileDelOptions.ConnectionName}] deleted.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error trying to delete a connection: " + ex.Message);
            }

        }
    }
}
