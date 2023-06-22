using Avro.Generic;

namespace converter;

public abstract class AvroRecord
{
    public static T MapToClass<T>(GenericRecord genericRecord) where T : class, new()
    {
        var outputObject = new T();
        var type = typeof(T);

        foreach (var field in genericRecord.Schema)
        {
            var prop = type.GetProperty(field.Name);
            if (prop != null)
            {
                prop.SetValue(outputObject, genericRecord[field.Name]);
            }
        }

        return outputObject;
    }

    public static GenericRecord ToGenericRecord<T>(T obj, Avro.Schema schema) where T : class
    {
        if (schema is not Avro.RecordSchema recordSchema)
        {
            throw new ArgumentException("Schema must be a record schema");
        }

        var genericRecord = new GenericRecord(recordSchema);

        foreach (var field in recordSchema.Fields)
        {
            var value = obj.GetType().GetProperty(field.Name)?.GetValue(obj);
            genericRecord.Add(field.Name, value);
        }

        return genericRecord;
    }
}