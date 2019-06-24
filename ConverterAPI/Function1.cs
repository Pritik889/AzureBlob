using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using ExcelDataReader;
using System.Data;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;

namespace ConverterAPI
{
    public static class Function1
    {
        //blob connection string
        static string connectionString = "DefaultEndpointsProtocol=https;AccountName=readfilepk;AccountKey=YTOhph+hsdewhtQk+RPvZTwBqZzjVcWide3rhPfkUW5BhgY61h8v7f+f+oY6QA+NRiaGcEUyglmK9GA==;EndpointSuffix=core.windows.net";

        [FunctionName("converttocsv")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            log.Info("C# HTTP trigger function processed a request.");
            dynamic data = await req.Content.ReadAsAsync<object>();
            JObject token = JsonConvert.DeserializeObject<JObject>(data.ToString());

            if (token != null)
            {
                DateTime aDate = DateTime.Now;
                //Get excel data
                var excelData = GetExcelBlobData((string)token["filename"], (string)token["containerName"]);
                //upload csv file on blob
                string csvfilename = (string)token["filename"];
                string[] file = csvfilename.Split('.');
                writtoCSVfile(file[0] + "-" + aDate.ToString("MM/dd/yyyy h:mm tt") + ".csv", (string)token["containerName"], excelData);
            }

            return req.CreateResponse(HttpStatusCode.OK, "File Converted successfully!!");
        }


        /// <summary>
        /// GetExcelBlobData
        /// Gets the Excel file Blob data and returns a dataset
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="connectionString"></param>
        /// <param name="containerName"></param>
        /// <returns></returns>
        private static DataSet GetExcelBlobData(string filename, string containerName)
        {
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            // Retrieve reference to a blob named "test.xlsx"
            CloudBlockBlob blockBlobReference = container.GetBlockBlobReference(filename);

            DataSet ds;
            using (var memoryStream = new MemoryStream())
            {
                //downloads blob's content to a stream
                blockBlobReference.DownloadToStream(memoryStream);
                var excelReader = ExcelReaderFactory.CreateOpenXmlReader(memoryStream);
                ds = excelReader.AsDataSet();
                excelReader.Close();
            }

            return ds;
        }


        /// <summary>
        /// Upload CSV file to blob
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="connectionString"></param>
        /// <param name="containerName"></param>
        /// <param name="ds"></param>
        private static void writtoCSVfile(string filename, string containerName, DataSet ds)
        {

            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);


            //string filename1 = "SampleData-11.csv";

            //get Blob reference
            CloudBlockBlob blockBlobReference = container.GetBlockBlobReference(filename);
            //cloudBlockBlob.Properties.ContentType =.ContentType;


            string ss = Converttocsvdata(ds);//Convert data in CSV formatM
            byte[] byteArray = Encoding.ASCII.GetBytes(ss);
            using (var memoryStream = new MemoryStream(byteArray))
            {
                //upload file to blob
                blockBlobReference.UploadFromStream(memoryStream);
            }
        }

        /// <summary>
        /// convert data in  csv format
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        private static string Converttocsvdata(DataSet result)
        {
            string csvData = "";
            int row_no = 0;

            while (row_no < result.Tables[0].Rows.Count) // ind is the index of table
                                                         // (sheet name) which you want to convert to csv
            {
                for (int i = 0; i < result.Tables[0].Columns.Count; i++)
                {
                    csvData += result.Tables[0].Rows[row_no][i].ToString() + "\t";
                }
                row_no++;
                csvData += "\n";
            }
            return csvData;
        }

    }
}
