using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using csharp.Model;

public class Program
{
    private static void Main()
    {
        var user = new User
                   {
                          name = "John",
                          favorite_number = 100L,
                          favorite_color = "green"
                     };

        var schema = (RecordSchema)user.Schema;
        var producerConfig = new ProducerConfig { BootstrapServers = "localhost:29092" };
        var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
        var avroSerializerConfig = new AvroSerializerConfig { BufferBytes = 1000 };

        var cts = new CancellationTokenSource();

        using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        using var producer =
            new ProducerBuilder<string, GenericRecord>(producerConfig)
               .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
                .Build();
        var record = new GenericRecord(schema);
        record.Add("name", "Chris");
        record.Add("favorite_number", 100L);
        record.Add("favorite_color", "red");
        
        producer.ProduceAsync("test", new Message<string, GenericRecord> { Key = record["name"].ToString(), Value = record })
                .ContinueWith(task =>
                              {
                                  Console.WriteLine(!task.IsFaulted
                                                        ? $"produced to: {task.Result.TopicPartitionOffset}"
                                                        : $"error producing message: {task.Exception.Message}");
                              },
                              cts.Token);
        Console.ReadKey();
    }
}
