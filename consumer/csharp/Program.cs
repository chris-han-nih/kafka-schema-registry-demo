using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using csharp;

public class Program
{
    private static async Task Main(string[] args)
    {


        var consumerConfig = new ConsumerConfig
                             {
                                 BootstrapServers = "localhost:29092",
                                 GroupId = "test-consumer-group",
                                 AutoOffsetReset = AutoOffsetReset.Earliest
                             };
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };

        var cts = new CancellationTokenSource();

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer =
            new ConsumerBuilder<string, GenericRecord>(consumerConfig)
               .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
               .Build();
        consumer.Subscribe("test");

        while (true)
        {
            Program.Consume(consumer, cts);
        }
    }

    private static void Consume(IConsumer<string, GenericRecord> consumer,
                                CancellationTokenSource cancellationTokenSource)
    {
        try
        {
            var consumeResult = consumer.Consume(cancellationTokenSource.Token);
            Console.WriteLine($"Consumed record with value {consumeResult.Message.Value}");

            var user = Converter.MapToClass<User>(consumeResult.Message.Value);
            Console.WriteLine($"name: {user.name}, favorite number: {user.favorite_number}, favorite color: {user.favorite_color}");
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
    }
}
