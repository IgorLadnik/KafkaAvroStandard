﻿using System;
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
                { KafkaPropNames.SchemaRegistryUrl, @"..\..\..\..\wwwroot\schema.json" /*http://localhost:9999/schema.json*/},
                { KafkaPropNames.Topic, "quick-start" },
                { KafkaPropNames.GroupId, "consumer-group" },
                { KafkaPropNames.Partition, 0 },
                { KafkaPropNames.Offset, 0 },
            };
            
            #endregion // Config

            #region Kafka Consumer

            var kafkaConsumer = new KafkaConsumer(config,
                                                  (key, value, dt) =>
                                                  {
                                                      Console.WriteLine();
                                                      Console.Write($"{key}  ->  ");
                                                      foreach (var field in value.Schema.Fields)
                                                          Console.Write($"{field.Name} = {value[field.Name]}  ");
                                                  },
                                                  s => Console.WriteLine(s))
                    .StartConsuming();

            #endregion // Kafka Consumer

            #region Create Kafka Producer 

            var kafkaProducer = new KafkaProducer(config,
                                                  s => Console.WriteLine(s));

            #endregion // Create Kafka Producer 

            #region Create and Send Objects 

            var count = 0;
            var startTime = new DateTime(2019, 10, 10);

            var timer = new Timer(_ =>
            {
                for (var i = 0; i < 10; i++)
                {
                    count++;

                    #region Create GenericRecord Object

                    var gr = new GenericRecord(kafkaProducer.RecordSchema);
                    gr.Add("SEQUENCE", count);
                    gr.Add("ID", count);
                    gr.Add("CategoryID", count);
                    gr.Add("YouTubeCategoryTypeID", count);
                    gr.Add("CreationTime", (DateTime.Now - startTime).Ticks / 10_000); // ms
                    
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

