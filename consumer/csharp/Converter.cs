namespace csharp;

using Avro.Generic;

public class Converter
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
}