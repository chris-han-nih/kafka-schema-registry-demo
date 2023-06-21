namespace csharp.Model;

using Avro;

public class User
{
	// public Schema Schema => Schema.Parse(File.ReadAllText("/Users/dave.han/NIH/kafka-schema-registry-demo/producer/csharp/Schema/User.avsc"));
	public Schema Schema => Schema.Parse(File.ReadAllText("../../../Schema/User.avsc"));
	public string name { get; set; }
	public long favorite_number { get; set; }
	public string favorite_color { get; set; }
}