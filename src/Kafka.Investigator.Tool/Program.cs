using CommandLine;
using Kafka.Investigator.Tool.Attributes;
using Kafka.Investigator.Tool.KafkaObjects;
using Kafka.Investigator.Tool.Options.ConsumerOptions;
using Kafka.Investigator.Tool.Options.ProfileOptions;
using Kafka.Investigator.Tool.ProfileManaging;
using Kafka.Investigator.Tool.UserInterations.ConsumerInterations;
using Kafka.Investigator.Tool.UserInterations.ProfileInteractions;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

IServiceProvider services = BuildSeviceProvider();
IMediator mediator = services.GetService<IMediator>();
int currentParserLevel = 0;

Console.ForegroundColor = ConsoleColor.White;
PrintPresentation(1);

ParseArgumentsWithSubVerbs(args, typeof(ConnectionOptions), 
                                 typeof(SchemaRegistryOptions), 
                                 typeof(ConsumerProfileOptions), 
                                 typeof(ConsumerOptions));

void ParseArgumentsWithSubVerbs(string[] args, params Type[] types)
{
    currentParserLevel++;

    var parser = new Parser(c =>
    {
        c.HelpWriter = Parser.Default.Settings.HelpWriter;
        c.IgnoreUnknownArguments = currentParserLevel == 1;
    });

    var parserResult = parser.ParseArguments(args, types);

    parserResult.WithParsed(async selectedOption =>
    {
        var selectedOptionType = selectedOption.GetType();

        Type[]? subVerbsTypes = Assembly.GetExecutingAssembly()
                                        .GetTypes()
                                        .Where(t => t.GetCustomAttribute<SubVerbAttribute>()?.BaseVerb == selectedOptionType)
                                        .ToArray();

        if (!subVerbsTypes.Any())
            await mediator?.Send(selectedOption);
        else
            ParseArgumentsWithSubVerbs(args[1..], subVerbsTypes);
    });
}

IServiceProvider BuildSeviceProvider()
{
    var services = new ServiceCollection();

    ConfigureServices(services);

    return services.BuildServiceProvider();
}

void ConfigureServices(IServiceCollection services)
{
    services.AddMediatR(Assembly.GetExecutingAssembly());
    services.AddSingleton<InvestigatorConsumerBuilder>();
    services.AddSingleton<InvestigatorSchemaRegistryBuilder>();
    services.AddSingleton<ProfileRepository>();
    services.AddSingleton<ConnectionAddInteraction>();
    services.AddSingleton<ConnectionDelInteraction>();
    services.AddSingleton<SchemaRegistryAddInteraction>();
    services.AddSingleton<SchemaRegistryDelInteraction>();
    services.AddSingleton<ConsumerProfileAddInteraction>();
    services.AddSingleton<ConsumerProfileDelInteraction>();
    services.AddSingleton<ConsumerStartInteraction>();
}

static void PrintPresentation(int option)
{
    var beforeColor = Console.ForegroundColor;
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.WriteLine(GetPresentation(1));
    Console.ForegroundColor = beforeColor;
}

static string GetPresentation(int option)
{
    string presentationText = "";

    if (option == 1)
    {
        presentationText = @"
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
    }
    else
    {
        presentationText = @"
█▄▀ ▄▀█ █▀▀ █▄▀ ▄▀█   █ █▄░█ █░█ █▀▀ █▀ ▀█▀ █ █▀▀ ▄▀█ ▀█▀ █▀█ █▀█
█░█ █▀█ █▀░ █░█ █▀█   █ █░▀█ ▀▄▀ ██▄ ▄█ ░█░ █ █▄█ █▀█ ░█░ █▄█ █▀▄

";
    }

    return presentationText;
}