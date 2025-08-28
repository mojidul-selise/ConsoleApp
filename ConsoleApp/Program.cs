using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoScriptExecutor
{
    class Program
    {
        private static async Task Main(string[] args)
        {
            //if (args.Length == 0)
            //{
            //    Console.WriteLine("Please provide a folder path containing JSON files.");
            //    return;
            //}

            //var folderPath = args[0];
            var folderPath = $"E:/SELISE/DB/Backup-loyalty/28082025/backup/backup/907d5798-07a3-45d2-987e-17607096daaa";

            if (!Directory.Exists(folderPath))
            {
                Console.WriteLine($"Directory not found: {folderPath}");
                return;
            }

            // MongoDB connection string
            var connectionString = "mongodb://localhost:27017"; // Update as needed
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase("importdatabase"); // Replace with your database name
            //var collection = database.GetCollection<BsonDocument>("YourCollectionName"); // Replace with your collection name

            // Get all JSON files in the specified folder
            var jsonFiles = Directory.GetFiles(folderPath, "*.json");

            foreach (var jsonFile in jsonFiles)
            {
                if (File.Exists(jsonFile))
                {
                    var collectionName = Path.GetFileNameWithoutExtension(jsonFile); // Get file name without extension
                    var collection = database.GetCollection<BsonDocument>(collectionName); // Use file name as collection name
                                                                                           // Empty the collection
                    await collection.DeleteManyAsync(FilterDefinition<BsonDocument>.Empty);
                    Console.WriteLine($"Emptied collection '{collectionName}'.");

                    //var script = await File.ReadAllTextAsync(jsonFile);
                    //var commands = script.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                    var commands = await File.ReadAllLinesAsync(jsonFile); // Read all lines from the file

                    List<BsonDocument> list = new List<BsonDocument>();
                    foreach (var command in commands)
                    {
                        if (!string.IsNullOrWhiteSpace(command))
                        {
                            try
                            {
                                var document = BsonSerializer.Deserialize<BsonDocument>(command);
                                list.Add(document);
                                //await collection.InsertOneAsync(document);
                                Console.WriteLine($"Inserted document from {jsonFile}: {document}");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Error inserting document from {jsonFile}: {ex.Message}");
                            }
                        }
                    }
                    if (list.Count > 0)
                    {
                        await collection.InsertManyAsync(list);
                    }                    
                }
                else
                {
                    Console.WriteLine($"File not found: {jsonFile}");
                }
            }
        }
    }
}