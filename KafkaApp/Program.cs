using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Avro.Generic;
using HttpClientLib;
using KafkaHelperLib;
using Newtonsoft.Json.Linq;

namespace KafkaApp
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Config

            var config = new Dictionary<string, object>
            {
                { KafkaPropNames.BootstrapServers, "localhost:9092" },
                //1 { KafkaPropNames.SchemaRegistryUrl, http://localhost:9999/schema.json },
                { KafkaPropNames.SchemaRegistryUrl, @"..\..\..\..\wwwroot\schema.json" }, //1
                { KafkaPropNames.Topic, "aa-topic" },
                { KafkaPropNames.GroupId, "aa-group" },
                { KafkaPropNames.Partition, 0 },
                { KafkaPropNames.Offset, 0 },
            };
            
            #endregion // Config

            #region Kafka Consumer

            var kafkaConsumer = new KafkaConsumer(config,
                                                  // Callback to process consumed (key -> value) item
                                                  (key, value, utcTimestamp) =>
                                                  {
                                                      Console.Write($"C#     {key}  ->  ");
                                                      foreach (var field in value.Schema.Fields)
                                                          Console.Write($"{field.Name} = {value[field.Name]}   ");
                                                      Console.WriteLine($"   {utcTimestamp}");
                                                  },
                                                  // Callback to process log message
                                                  s => Console.WriteLine(s))
                    .StartConsuming();

            #endregion // Kafka Consumer

            #region Create Kafka Producer 

            var kafkaProducer = new KafkaProducer(config,
                                                  // Callback to process log message
                                                  s => Console.WriteLine(s));

            #endregion // Create Kafka Producer 

            #region Create and Send Objects 

            var count = 0;
            var rnd = new Random(15);
            
            var timer = new Timer(_ =>
            {
                for (var i = 0; i < 10; i++)
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

            timer.Dispose();
            kafkaProducer.Dispose();
            kafkaConsumer.Dispose();
        }

        private static IDictionary<string, object> GetSchemaString(string schemaRegistryUrl)
        {
            try
            {
                var str = Encoding.Default.GetString(new HttpClient().Get(schemaRegistryUrl, 100))
                                       .Replace(" ", string.Empty).Replace("\n", "").Replace("\r", "").Replace("\t", "").Replace("\\", "");

                var jOb = JObject.Parse(str);
                var dctProp = new Dictionary<string, object>();
                foreach (var property in jOb.Properties())
                    dctProp[property.Name] = property.Value;

                return dctProp;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return null;
            }
        }
    }
}

