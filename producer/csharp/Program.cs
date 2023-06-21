using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using csharp.Model;

public class Program
{
    private static async Task Main()
    {
        var user = new User
                   {
                          name = "Dave",
                          favorite_number = 100L,
                          favorite_color = "blue"
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

        var record = Program.ConvertToGenericRecord(user, schema);

        await producer.ProduceAsync("test",
                                    new Message<string, GenericRecord>
                                    {
                                        Key = record["name"].ToString(),
                                        Value = record
                                    })
                      .ContinueWith(task =>
                                    {
                                        Console.WriteLine(!task.IsFaulted
                                                              ? $"produced to: {task.Result.TopicPartitionOffset}"
                                                              : $"error producing message: {task.Exception.Message}");
                                    },
                                    cts.Token);
    }
    
    public static GenericRecord ConvertToGenericRecord<T>(T obj, Avro.Schema schema) where T : class
    {
        if (schema is not RecordSchema recordSchema)
        {
            throw new ArgumentException("Schema must be a record schema");
        }

        var genericRecord = new GenericRecord(recordSchema);

        foreach (var field in recordSchema.Fields)
        {
            var prop = typeof(T).GetProperty(field.Name);

            if (prop == null)
            {
                continue;
            }

            var value = prop.GetValue(obj);
            genericRecord.Add(field.Name, value);
        }

        return genericRecord;
    }
}
