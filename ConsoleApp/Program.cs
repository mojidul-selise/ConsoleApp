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
            //var folderPath = $"E:/SELISE/DB/Backup-loyalty/28082025/backup/backup/907d5798-07a3-45d2-987e-17607096daaa";
            var folderPath = $"E:/MT/DB/Mongo/RequiredForNewDatabase";
            if (!Directory.Exists(folderPath))
            {
                Console.WriteLine($"Directory not found: {folderPath}");
                return;
            }

            // MongoDB connection string
            var connectionString = "mongodb://localhost:27017"; // Update as needed
            //import collections
            IMongoDatabase database = GetMongoDatabaseConnection(connectionString, "28d7f20c-11e6-44f0-a3ae-3178d431534f");
            await ImportAllCollectionFromLocation(folderPath, database);

            // import collction from one database to another database
            //IMongoDatabase sourceDatabase = GetMongoDatabaseConnection(connectionString, "907d5798-07a3-45d2-987e-17607096daaa");
            //IMongoDatabase targetDatabase = GetMongoDatabaseConnection(connectionString, "28d7f20c-11e6-44f0-a3ae-3178d431534f");
            //await ImportFromOneDatabaseToAnotherDatabase(sourceDatabase, targetDatabase);

            // delete all data from collections
            //IMongoDatabase database = GetMongoDatabaseConnection(connectionString, "28d7f20c-11e6-44f0-a3ae-3178d431534f");
            //var collections = await database.ListCollectionNamesAsync();
            //List<string> existingCollections = collections.ToList();
            //await DeleteAllDataFromCollections(database, existingCollections);
                        
        }

        private static async Task DeleteAllDataFromCollections(IMongoDatabase database, List<string> collectionNames)
        {
            if (collectionNames.Any() == false) return;
            foreach (var collectionName in collectionNames)
            {
                var collection = database.GetCollection<BsonDocument>(collectionName);
                await collection.DeleteManyAsync(new BsonDocument()); // Deletes all documents in the collection
            }
        }

        private static async Task ImportFromOneDatabaseToAnotherDatabase(IMongoDatabase sourceDatabase, IMongoDatabase targetDatabase)
        {
            var collections = await sourceDatabase.ListCollectionNamesAsync();
            while (await collections.MoveNextAsync())
            {
                foreach (var collectionName in collections.Current)
                {
                    var sourceCollection = sourceDatabase.GetCollection<BsonDocument>(collectionName);
                    var documents = await sourceCollection.Find(new BsonDocument()).ToListAsync();
                    var targetCollection = targetDatabase.GetCollection<BsonDocument>(collectionName);
                    await targetCollection.InsertManyAsync(documents);
                }
            }
        }

        private static async Task ImportAllCollectionFromLocation(string folderPath, IMongoDatabase database)
        {
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

        private static IMongoDatabase GetMongoDatabaseConnection(string connectionString, string databaseName)
        {
            var client = new MongoClient(connectionString);
            var database = client.GetDatabase(databaseName);
            return database;
        }
    }
}