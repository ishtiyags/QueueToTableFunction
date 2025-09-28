using Azure;
using Azure.Data.Tables;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace QueueToTableFunctionApp
{
    public class QueueToTableFunction
    {
        private readonly ILogger _logger;
        private readonly TableClient _tableClient;

        public QueueToTableFunction(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<QueueToTableFunction>();

            // Read table connection string from environment variables
            string connectionString = Environment.GetEnvironmentVariable("AZURE_TABLES_CONNECTION_STRING")!;
            string tableName = "MyTable";

            var serviceClient = new TableServiceClient(connectionString);
            _tableClient = serviceClient.GetTableClient(tableName);

            // Create table if it doesn't exist
            _tableClient.CreateIfNotExists();
        }

        [Function("QueueToTableFunction")]
        public async Task Run([QueueTrigger("myqueue", Connection = "AZURE_STORAGE_CONNECTION_STRING")] string queueMessage)
        {
            _logger.LogInformation($"Queue message received: {queueMessage}");

            var entity = new TableEntity("partition1", Guid.NewGuid().ToString())
            {
                { "Message", queueMessage }
            };

            await _tableClient.AddEntityAsync(entity);
            _logger.LogInformation("Message written to Table Storage successfully.");
        }
    }
}

