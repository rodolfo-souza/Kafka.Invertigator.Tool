// See https://aka.ms/new-console-template for more information

using CommandLine;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using Kafka.Investigator.Tool.UserInterations.ProfileInteractions;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;


PrintPresentation();

var parsedValue = Parser.Default.ParseArguments<ConnectionAddOptions, 
                                                ConnectionListOptions,
                                                ConnectionDelOptions,
                                                SchemaRegistryAddOptions,
                                                SchemaRegistryListOptions,
                                                SchemaRegistryDelOptions,
                                                ConsumerProfileAddOptions,
                                                ConsumerProfileListOptions,
                                                ConsumerProfileStartOptions,
                                                ConsumerStartOptions>(args);

parsedValue.WithParsed(async options => await ExecutarAsync(options))
           .WithNotParsed(errors => Console.WriteLine(string.Join(", ", errors.Select(e => e.Tag))));


// Solicita ao MediatR que enderece o processamento da option. 
async Task ExecutarAsync(object option)
{
    var services = new ServiceCollection();

    ConfigureServices(services);

    var serviceProvider = services.BuildServiceProvider();

    await ExecuteOption(option, serviceProvider);
}

void ConfigureServices(IServiceCollection services)
{
    // Configura serviços da aplicação.

    services.AddMediatR(Assembly.GetExecutingAssembly());
    services.AddSingleton<InvestigatorConsumerBuilder>();
    services.AddSingleton<InvestigatorSchemaRegistryBuilder>();
    services.AddSingleton<ProfileRepository>();
    services.AddSingleton<ConnectionAddInteraction>();
    services.AddSingleton<ConnectionDelInteraction>();
    services.AddSingleton<SchemaRegistryAddInteraction>();
    services.AddSingleton<SchemaRegistryDelInteraction>();
    services.AddSingleton<ConsumerStartInteraction>();
    services.AddSingleton<ConsumerProfileAddInteraction>();
}

static async Task ExecuteOption(object options, ServiceProvider serviceProvider)
{
    var mediator = serviceProvider.GetService<IMediator>();

    if (mediator == null)
        throw new Exception("MediatR was not configured properly.");

    await mediator.Send(options);
}

static void PrintPresentation()
{
    var text = @"

██╗░░██╗░█████╗░███████╗██╗░░██╗░█████╗░
██║░██╔╝██╔══██╗██╔════╝██║░██╔╝██╔══██╗
█████═╝░███████║█████╗░░█████═╝░███████║
██╔═██╗░██╔══██║██╔══╝░░██╔═██╗░██╔══██║
██║░╚██╗██║░░██║██║░░░░░██║░╚██╗██║░░██║
╚═╝░░╚═╝╚═╝░░╚═╝╚═╝░░░░░╚═╝░░╚═╝╚═╝░░╚═╝

██╗███╗░░██╗██╗░░░██╗███████╗░██████╗████████╗██╗░██████╗░░█████╗░████████╗░█████╗░██████╗░
██║████╗░██║██║░░░██║██╔════╝██╔════╝╚══██╔══╝██║██╔════╝░██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗
██║██╔██╗██║╚██╗░██╔╝█████╗░░╚█████╗░░░░██║░░░██║██║░░██╗░███████║░░░██║░░░██║░░██║██████╔╝
██║██║╚████║░╚████╔╝░██╔══╝░░░╚═══██╗░░░██║░░░██║██║░░╚██╗██╔══██║░░░██║░░░██║░░██║██╔══██╗
██║██║░╚███║░░╚██╔╝░░███████╗██████╔╝░░░██║░░░██║╚██████╔╝██║░░██║░░░██║░░░╚█████╔╝██║░░██║
╚═╝╚═╝░░╚══╝░░░╚═╝░░░╚══════╝╚═════╝░░░░╚═╝░░░╚═╝░╚═════╝░╚═╝░░╚═╝░░░╚═╝░░░░╚════╝░╚═╝░░╚═╝
";
    var beforeColor = Console.ForegroundColor;
    Console.ForegroundColor = ConsoleColor.Blue;
    Console.WriteLine(text);
    Console.ForegroundColor = beforeColor;
}