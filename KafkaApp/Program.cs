using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Threading;
using Avro.Generic;
using KafkaHelperLib;
using System.Net;

namespace KafkaApp
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Config

            const string schemaLocation = "schema.json";
            //const string schemaLocation = "http://localhost:9999/schema.json";

            const string topic = "test1-topic";
            const int version = 11;

            var ipAddress = Dns.GetHostEntry(Dns.GetHostName())
                    .AddressList
                    .First(addr => addr.AddressFamily == AddressFamily.InterNetwork);

            var config = new Dictionary<string, object>
            {
                { KafkaPropNames.BootstrapServers, "localhost:9092" },
                { KafkaPropNames.SchemaRegistryUrl, $"http://localhost:8081/subjects/{topic}/versions/{version}/schema" },
                { KafkaPropNames.Topic, topic },
                { KafkaPropNames.GroupId, $"{ipAddress}-my-group" },
                { KafkaPropNames.Partition, 0 },
                { KafkaPropNames.Offset, 0 },
                { KafkaPropNames.SchemaRegistryRequestTimeoutMs, 5000 },
            };

            #endregion // Config

            #region Kafka Consumer

            using var kafkaConsumer = new KafkaConsumer(config,
                                                  // Callback to process consumed (key -> value) item
                                                  (key, value, utcTimestamp) =>
                                                  {
                                                      Console.Write($"C#     {key}  ->  ");
                                                      foreach (var field in value.Schema.Fields)
                                                          Console.Write($"{field.Name} = {value[field.Name]}   ");
                                                      Console.WriteLine($"   {utcTimestamp}");
                                                  },
                                                  // Callback to process log message
                                                  s => Console.WriteLine(s));

            #endregion // Kafka Consumer

            #region Create Kafka Producer 

            using var kafkaProducer = new KafkaProducer(schemaLocation, config,
                                                  // Callback to process log message
                                                  s => Console.WriteLine(s));

            #endregion // Create Kafka Producer 

            #region Create and Send Objects 

            var count = 0;
            var rnd = new Random(15);

            using var timer = new Timer(_ =>
            {
                for (var i = 0; i < 1; i++)
                {
                    count++;

                    #region Create GenericRecord Object

                    var gr = new GenericRecord(kafkaProducer.RecordSchema);
                    gr.Add("Id", count);
                    gr.Add("Name", $"{config[KafkaPropNames.GroupId]}-{count}");
                    gr.Add("BatchId", (count / 10) + 1);
                    gr.Add("TextData", "Some text data");
                    gr.Add("NumericData", (long)rnd.Next(0, 100_000));
                   
                    #endregion // Create GenericRecord Object

                    kafkaProducer.SendAsync($"{count}", gr).Wait();
                }
            },
            null, 0, 5000);

            #endregion // Create and Send Objects 

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }
    }
}

