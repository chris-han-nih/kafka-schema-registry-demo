using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using converter;
using model;

var producerConfig = new ProducerConfig { BootstrapServers = "localhost:29092" };
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
var avroSerializerConfig = new AvroSerializerConfig { BufferBytes = 1000 };

var cts = new CancellationTokenSource();

using var sr = new StreamReader("../../../Avro/user.avsc");
var schema = Avro.Schema.Parse(sr.ReadToEnd());

using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, avroSerializerConfig))
                    .Build();

while (true)
{
    await Task.Delay(100, cts.Token);

    var user = new User
               {
                   name = Guid.NewGuid().ToString(),
                   favorite_number = new Random().Next(0, 100),
               };

    var record = AvroRecord.ToGenericRecord(user, (RecordSchema)schema);
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