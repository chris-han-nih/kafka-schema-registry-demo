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
        await new HostBuilder().ConfigureServices((_, services) =>
                                                  {
                                                      services.AddOptions();
                                                      services
                                                         .Configure<ProducerConfiguration>(config.GetSection("KafkaConfiguration"));
                                                      services.AddHostedService<MessageGenerateService>();
                                                      services.AddHostedService<KafkaProducerService>();
                                                  })
                               .RunConsoleAsync();
    }
}
