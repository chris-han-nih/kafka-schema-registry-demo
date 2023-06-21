using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

public class Program
{
    static async Task Main(string[] args)
    {
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };

        var cts = new CancellationTokenSource();


        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var consumer =
            new ConsumerBuilder<string, GenericRecord>(new ConsumerConfig
                                                       {
                                                           BootstrapServers = "localhost:29092",
                                                           GroupId = "test-consumer-group",
                                                           AutoOffsetReset = AutoOffsetReset.Earliest
                                                       })
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
                    
                    var record = consumeResult.Message.Value;
                    Console.WriteLine($"name: {record["name"]}, favorite number: {record["favorite_number"]}, favorite color: {record["favorite_color"]}");
                    
                    // https://github.com/AdrianStrugala/AvroConvert/blob/master/docs/Documentation.md
                    // var user = AvroConvert.DeserializeHeadless<User>(record, schema);
                    // Console.WriteLine($"name: {user.name}, favorite number: {user.favorite_number}, favorite color: {user.favorite_color}");
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
}
