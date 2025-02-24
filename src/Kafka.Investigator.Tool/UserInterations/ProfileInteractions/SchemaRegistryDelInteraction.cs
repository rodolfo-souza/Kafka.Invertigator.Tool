﻿using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using MediatR;

namespace Kafka.Investigator.Tool.UserInterations.ProfileInteractions
{
    internal class SchemaRegistryDelInteraction : IRequestHandler<SchemaRegistryDelOptions>
    {
        private readonly ProfileRepository _profileRepository;

        public SchemaRegistryDelInteraction(ProfileRepository profileRepository)
        {
            _profileRepository = profileRepository;
        }

        public Task<Unit> Handle(SchemaRegistryDelOptions request, CancellationToken cancellationToken)
        {
            DelSchemaRegistry(request);
            return Task.FromResult(Unit.Value);
        }

        public void DelSchemaRegistry(SchemaRegistryDelOptions options)
        {
            try
            {
                var existingSchemaRegistry = _profileRepository.GetSchemaRegistry(options.SchemaRegistryName);

                if (existingSchemaRegistry == null)
                {
                    UserInteractionsHelper.WriteWarning($"SchemaRegistry [{options.SchemaRegistryName}] not found.");
                    return;
                }

                if (UserInteractionsHelper.RequestUserResponse($"Confirm EXCLUSION of profile [{options.SchemaRegistryName}]? Y/N", color: ConsoleColor.Yellow) != "Y")
                    return;

                _profileRepository.DelSchemaRegistry(existingSchemaRegistry);

                UserInteractionsHelper.WriteSuccess($"Profile [{options.SchemaRegistryName}] deleted.");
            }
            catch (Exception ex)
            {
                UserInteractionsHelper.WriteError("Error trying to delete a SchemaRegistry: " + ex.Message);
            }

        }
    }
}
