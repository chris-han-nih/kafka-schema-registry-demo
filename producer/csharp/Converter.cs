namespace csharp;

public sealed class Converter
{
    public static Avro.Generic.GenericRecord ToGenericRecord<T>(T obj, Avro.Schema schema) where T : class
    {
        if (schema is not Avro.RecordSchema recordSchema)
        {
            throw new ArgumentException("Schema must be a record schema");
        }

        var genericRecord = new Avro.Generic.GenericRecord(recordSchema);

        foreach (var field in recordSchema.Fields)
        {
            var value = obj.GetType().GetProperty(field.Name)?.GetValue(obj);
            genericRecord.Add(field.Name, value);
        }

        return genericRecord;
    }
}