using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class ConnectionDelInteraction : IRequestHandler<ConnectionDelOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public ConnectionDelInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(ConnectionDelOptions profileDelOptions, CancellationToken cancellationToken)
        {
            DelConnection(profileDelOptions);

            return Task.FromResult(Unit.Value);
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

                var response = UserInteractionsHelper.RequestYesNoResponse($"Confirm EXCLUSION of connection [{profileDelOptions.ConnectionName}]?");
                if (response != "Y")
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
