namespace csharp.Model;

using Avro;

public class User
{
	public Schema Schema => Schema.Parse(File.ReadAllText("../../../Schema/User.avsc"));
	public string name => Guid.NewGuid().ToString();
	public long favorite_number => Math.Abs(Guid.NewGuid().GetHashCode());
}