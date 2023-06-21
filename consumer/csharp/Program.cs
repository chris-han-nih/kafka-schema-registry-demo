using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

public class Program
{
    static async Task Main(string[] args)
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
        try
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed record with value {consumeResult.Message.Value}");
                    
                    var user = Program.MapToClass<User>(consumeResult.Message.Value);
                    Console.WriteLine($"name: {user.name}, favorite number: {user.favorite_number}, favorite color: {user.favorite_color}");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException e)
        {
            consumer.Close();
        }
    }

    private static T MapToClass<T>(GenericRecord genericRecord) where T : class, new()
    {
        var outputObject = new T();
        var type = typeof(T);

        foreach (var field in genericRecord.Schema)
        {
            var prop = type.GetProperty(field.Name);
            if (prop != null && genericRecord.TryGetValue(field.Name, out var fieldValue))
            {
                prop.SetValue(outputObject, Convert.ChangeType(fieldValue, prop.PropertyType), null);
            }
        }

        return outputObject;
    }
}
