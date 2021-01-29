using System;
using System.Threading.Tasks;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosDB
{
    class Program
    {
        private static readonly string endpointUri = "";
        private static readonly string primaryKey = "";
        private CosmosClient cosmosClient;
        private Database database;
        private Container container;
        private string databaseName = "FamilyDatabase";
        private string containerName= "FamilyContainer";

        public static async Task Main(string[] args)
        {
            try
            {
                Console.WriteLine("Beginning operations....");
                var program = new Program();
                await program.GetStartedDemoAsync();
            }
            catch (CosmosException ex)
            {
                var baseException = ex.GetBaseException();
                Console.WriteLine($"{ex.StatusCode} An error occured: {ex}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An error occured: {ex}");
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        public async Task GetStartedDemoAsync()
        {
            cosmosClient = new CosmosClient(endpointUri, primaryKey);
            await CreateDatabaseAsync();
            await CreateContainerAsync();
            await AddItemsToContainerAsync();
            await QueryItemsAsync();
            await ReplaceFamilyItemAsync();
            await DeleteFamilyItemAsync();
            await DeleteDatabaseAndCleanupAsync();
        }
        private async Task CreateDatabaseAsync()
        {
            database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
            Console.WriteLine($"Created Database: {databaseName}");
        }
        private async Task CreateContainerAsync()
        {
            container = await database
                .CreateContainerIfNotExistsAsync(containerName, "/LastName");
            
            Console.WriteLine($"Created Container: {containerName}");
        }
        private async Task AddItemsToContainerAsync()
        {
            Family vanAppeldoornFamily = new Family
            {
                Id = "Appeldoorn.1",
                LastName = "van Appeldoorn",
                Parents = new Parent[]
                {
                    new Parent { FirstName = "Bertus" },
                    new Parent { FirstName = "Mien" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FirstName = "Erikson Salvador",
                        Gender = "Male",
                        Age = 17,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Nacho" },
                            new Pet { GivenName = "Ricito"}
                        }
                    }
                },
                Address = new Address 
                { 
                    Street = "Ter Maatenlaan 23",
                    Zipcode = "3931 WE",
                    City = "Woudenberg"
                },
                IsRegistered = false
            };

            try
            {
                ItemResponse<Family> vanAppeldoornFamilyResponse = 
                    await container.ReadItemAsync<Family>(vanAppeldoornFamily.Id, new PartitionKey(vanAppeldoornFamily.LastName));
                Console.WriteLine($"Item in database with id: {vanAppeldoornFamily.Id} already exists");
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                ItemResponse<Family> vanAppeldoornFamilyResponse = 
                    await container.CreateItemAsync(vanAppeldoornFamily, new PartitionKey(vanAppeldoornFamily.LastName));

                Console.WriteLine($"Created item in database with id: {vanAppeldoornFamilyResponse.Resource.Id} Operation consumed {vanAppeldoornFamilyResponse.RequestCharge} RUs.");
            }

            Family wakefieldFamily = new Family
            {
                Id = "Wakefield.7",
                LastName = "Wakefield",
                Parents = new Parent[]
                {
                    new Parent { FamilyName = "Wakefield", FirstName = "Robin" },
                    new Parent { FamilyName = "Miller", FirstName = "Ben" }
                },
                Children = new Child[]
                {
                    new Child
                    {
                        FamilyName = "Merriam",
                        FirstName = "Jesse",
                        Gender = "female",
                        Age = 8,
                        Pets = new Pet[]
                        {
                            new Pet { GivenName = "Goofy" },
                            new Pet { GivenName = "Shadow" }
                        }
                    },
                    new Child
                    {
                        FamilyName = "Miller",
                        FirstName = "Lisa",
                        Gender = "female",
                        Age = 1
                    }
                },
                Address = new Address 
                { Street = "Europe boulevard 67", 
                  Zipcode = "1010AB", 
                  City = "New York" 
                },
                IsRegistered = true
            };

            try
            {
                ItemResponse<Family> wakefieldFamilyResponse = 
                    await container.ReadItemAsync<Family>(wakefieldFamily.Id, new PartitionKey(wakefieldFamily.LastName));
                Console.WriteLine($"Item in database with id: {wakefieldFamilyResponse.Resource.Id} already exists");
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                ItemResponse<Family> wakefieldFamilyResponse = 
                    await container.CreateItemAsync(wakefieldFamily, new PartitionKey(wakefieldFamily.LastName));

                Console.WriteLine($"Created item in database with id: {wakefieldFamilyResponse.Resource.Id} Operation consumed {wakefieldFamilyResponse.RequestCharge} RUs.");
            }
        }
        private async Task QueryItemsAsync()
        {
            var sqlQueryText = "SELECT * FROM c WHERE c.LastName = 'van Appeldoorn'";

            Console.WriteLine("Running query: {0}\n", sqlQueryText);

            var queryDefinition = new QueryDefinition(sqlQueryText);
            var queryResultSetIterator = container.GetItemQueryIterator<Family>(queryDefinition);

            List<Family> families = new List<Family>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<Family> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                foreach (Family family in currentResultSet)
                {
                    families.Add(family);
                    Console.WriteLine("\tRead {0}\n", family);
                }
            }
        }
        private async Task ReplaceFamilyItemAsync()
        {
            ItemResponse<Family> vanAppeldoornFamilyResponse = 
                await container.ReadItemAsync<Family>("Appeldoorn.1", new PartitionKey("van Appeldoorn"));
            var itemBody = vanAppeldoornFamilyResponse.Resource;

            itemBody.IsRegistered = true;
            itemBody.Children[0].Age = 18;

            vanAppeldoornFamilyResponse = 
                await container.ReplaceItemAsync(itemBody, itemBody.Id, new PartitionKey(itemBody.LastName));
            Console.WriteLine($"Updated Family [{itemBody.LastName},{itemBody.Id}].\n \tBody is now: {vanAppeldoornFamilyResponse.Resource}");
        }
        private async Task DeleteFamilyItemAsync()
        {
            var partitionKeyValue = "van Appeldoorn";
            var familyId = "Appeldoorn.1";

            var wakefieldFamilyResponse = 
                await container.DeleteItemAsync<Family>(familyId, new PartitionKey(partitionKeyValue));
            Console.WriteLine($"Deleted Family [{partitionKeyValue},{familyId}]");
        }
        private async Task DeleteDatabaseAndCleanupAsync()
        {
            DatabaseResponse databaseResourceResponse = 
                await database.DeleteAsync();

            Console.WriteLine($"Deleted Database: {databaseName}");
            cosmosClient.Dispose();
        }
    }
}
