namespace producer.Services;

using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Configurations;

public sealed class KafkaProducerService: IHostedService, IDisposable
{
    private static List<Dictionary<string, string>> _messages = new ();
    private IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private readonly ProducerConfiguration _producerConfig; 
    public KafkaProducerService(ILogger<KafkaProducerService> logger, IOptions<ProducerConfiguration> producerConfig)
    {
        _logger = logger ?? throw new ArgumentException(nameof(logger));
        _producerConfig = producerConfig.Value ?? throw new ArgumentException(nameof(producerConfig));

        Init();
    }

    public static void AddMessage(string key, string value)
    {
        KafkaProducerService._messages.Add(new Dictionary<string, string>{{key, value}});
    }

    private void Init()
    {
        var config = new ProducerConfig
                     {
                         BootstrapServers = _producerConfig.Brokers,
                         ClientId = _producerConfig.ClientId,
                         EnableDeliveryReports = false,
                         QueueBufferingMaxMessages = 1000000,
                         QueueBufferingMaxKbytes = 1000000,
                         BatchNumMessages = 500,
                         Acks = Acks.None,
                         DeliveryReportFields = "none"
                     };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.CompletedTask;
                return;
            }

            try
            {
                _logger.LogInformation("Kafka Producer Service is starting");
                await Produce(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Kafka Producer Service is stopping");
        _producer.Flush(cancellationToken);
        
        await Task.CompletedTask;
    }

    private async Task Produce(CancellationToken cancellationToken)
    {
        try
        {
            if (cancellationToken.IsCancellationRequested) return;
            
            using var scope = _logger.BeginScope("Kafka App Produce");
            foreach (var m in KafkaProducerService._messages.FirstOrDefault())
            {
                _logger.LogInformation("Producing message {m}");
                // var msg = new Message<string, string>
                //           {
                //               Value = m.Value
                //           };
                // await _producer.ProduceAsync("test", msg, cancellationToken)
                //                .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error producing message");
        }
    }

    public void Dispose()
    {
        _producer.Dispose();
    }
}