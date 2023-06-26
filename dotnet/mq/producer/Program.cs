namespace producer;

using Configurations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using producer.Services;

public class Program
{
    private static async Task Main()
    {
        var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                .AddJsonFile("config.json", optional: false);
        var config = builder.Build();
        await Program.CreateHostBuilder(config).Build().RunAsync();
    }
    public static IHostBuilder CreateHostBuilder(IConfigurationRoot config) =>
        Host.CreateDefaultBuilder()
            .ConfigureServices((_, services) =>
                               {
                                   services.AddOptions();
                                   services.Configure<ProducerConfiguration>(config.GetSection("KafkaConfiguration"));
                                   services.AddHostedService<KafkaProducerService>();
                                   services.AddHostedService<MessageGenerateService>();
                               });
}
