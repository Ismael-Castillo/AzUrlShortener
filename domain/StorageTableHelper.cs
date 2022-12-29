using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace Cloud5mins.domain
{
	public class StorageTableHelper
	{
		private string StorageConnectionString { get; set; }

		public StorageTableHelper() { }

		public StorageTableHelper(string storageConnectionString)
		{
			StorageConnectionString = storageConnectionString;
		}
		public CloudStorageAccount CreateStorageAccountFromConnectionString()
		{
			CloudStorageAccount storageAccount = CloudStorageAccount.Parse(this.StorageConnectionString);
			return storageAccount;
		}

		private CloudTable GetTable(string tableName)
		{
			CloudStorageAccount storageAccount = this.CreateStorageAccountFromConnectionString();
			CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());
			CloudTable table = tableClient.GetTableReference(tableName);
			table.CreateIfNotExists();

			return table;
		}
		private CloudTable GetUrlsTable()
		{
			CloudTable table = GetTable("UrlsDetails");
			return table;
		}

		private CloudTable GetStatsTable()
		{
			CloudTable table = GetTable("ClickStats");
			return table;
		}

		public async Task<ShortUrlEntity> GetShortUrlEntity(ShortUrlEntity row)
		{
			TableOperation selOperation = TableOperation.Retrieve<ShortUrlEntity>(row.PartitionKey, row.RowKey);
			TableResult result = await GetUrlsTable().ExecuteAsync(selOperation);
			ShortUrlEntity eShortUrl = result.Result as ShortUrlEntity;
			return eShortUrl;
		}

		public async Task SaveClickStatsEntity(ClickStatsEntity newStats)
		{
			TableOperation insOperation = TableOperation.InsertOrMerge(newStats);
			TableResult result = await GetStatsTable().ExecuteAsync(insOperation);
		}

		public async Task<ShortUrlEntity> SaveShortUrlEntity(ShortUrlEntity newShortUrl)
		{
			TableOperation insOperation = TableOperation.InsertOrMerge(newShortUrl);
			TableResult result = await GetUrlsTable().ExecuteAsync(insOperation);
			ShortUrlEntity eShortUrl = result.Result as ShortUrlEntity;
			return eShortUrl;
		}

		public async Task<bool> IfShortUrlEntityExist(ShortUrlEntity row)
		{
			ShortUrlEntity eShortUrl = await GetShortUrlEntity(row);
			return (eShortUrl != null);
		}

		public async Task<int> GetNextTableId()
		{
			//Get current ID
			TableOperation selOperation = TableOperation.Retrieve<NextId>("1", "KEY");
			TableResult result = await GetUrlsTable().ExecuteAsync(selOperation);
			NextId entity = result.Result as NextId;

			if (entity == null)
			{
				entity = new NextId
				{
					PartitionKey = "1",
					RowKey = "KEY",
					Id = 1024
				};
			}
			entity.Id++;

			//Update
			TableOperation updOperation = TableOperation.InsertOrMerge(entity);

			// Execute the operation.
			await GetUrlsTable().ExecuteAsync(updOperation);

			return entity.Id;
		}

		public async Task<bool> IfShortUrlEntityExistByVanity(string vanity)
		{
			ShortUrlEntity shortUrlEntity = await GetShortUrlEntityByVanity(vanity);
			return (shortUrlEntity != null);
		}

		public async Task<ShortUrlEntity> GetShortUrlEntityByVanity(string vanity)
		{
			var tblUrls = GetUrlsTable();
			TableContinuationToken token = null;
			ShortUrlEntity shortUrlEntity = null;
			do
			{
				TableQuery<ShortUrlEntity> query = new TableQuery<ShortUrlEntity>().Where(
					filter: TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, vanity));
				var queryResult = await tblUrls.ExecuteQuerySegmentedAsync(query, token);
				shortUrlEntity = queryResult.Results.FirstOrDefault();
			} while (token != null);

			return shortUrlEntity;
		}

	}
}