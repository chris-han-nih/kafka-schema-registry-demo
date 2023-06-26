namespace producer.Services;

using Microsoft.Extensions.Hosting;

public sealed class MessageGenerateService: IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.CompletedTask;
                return;
            }
            Thread.Sleep(100);

            var msg = Guid.NewGuid().ToString();
            KafkaProducerService.AddMessage(msg[..4], msg);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
    }
}