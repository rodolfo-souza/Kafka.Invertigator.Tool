using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Kafka.Investigator.Tool.OptionsHandlers
{
    internal class ConsumerOptionsHandler : INotificationHandler<ConsumeStartOption>
    {
        private readonly ConsumerStartInteraction _consumerStartInteraction;

        public ConsumerOptionsHandler(ConsumerStartInteraction consumerStartInteraction)
        {
            _consumerStartInteraction = consumerStartInteraction;
        }

        public Task Handle(ConsumeStartOption consumerOptions, CancellationToken cancellationToken)
        {
            _consumerStartInteraction.StartConsume(consumerOptions, cancellationToken);

            return Task.CompletedTask;
        }
    }
}
