using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using csharp;
using csharp.Model;

var producerConfig = new ProducerConfig { BootstrapServers = "localhost:29092" };
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
var avroSerializerConfig = new AvroSerializerConfig { BufferBytes = 1000 };

    var cts = new CancellationTokenSource();

    using var schemaRegistry =
        new CachedSchemaRegistryClient(schemaRegistryConfig);
    using var producer =
        new ProducerBuilder<string, GenericRecord>(producerConfig)
           .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
           .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
           .Build();

while (true)
{
    await Task.Delay(100, cts.Token);
    
    var user = new User();

    var record = Converter.ToGenericRecord(user, (RecordSchema)user.Schema);
    var message = new Message<string, GenericRecord> { Key = user.name, Value = record };

    await producer.ProduceAsync("test", message)
                  .ContinueWith(task =>
                                {
                                    Console.WriteLine(!task.IsFaulted
                                                          ? $"produced to: {task.Result.TopicPartitionOffset}"
                                                          : $"error producing message: {task.Exception?.Message}");
                                },
                                cts.Token);
}