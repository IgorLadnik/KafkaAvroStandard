using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaHelperLib
{
    public class KafkaProducer : IDisposable
    {
        #region ReadOnly 

        private readonly TimeSpan MaxLiveTimeSpan = TimeSpan.FromMinutes(3);

        #endregion // ReadOnly

        #region Vars

        private Queue<KeyValuePair<string, GenericRecord>> _quePair = new Queue<KeyValuePair<string, GenericRecord>>();

        private IProducer<string, GenericRecord> _producer;
        private string _topic;
        private Dictionary<string, object> _config;
        private DateTime _creationTime;
        
        private Action<string> _logger;
        private ISchemaRegistryClient _schemaRegistry;

        public RecordSchema RecordSchema { get; private set; }

        #endregion // Vars

        #region Ctor & Create producer

        public KafkaProducer(string schemaLocation,
                             Dictionary<string, object> config,
                             Action<string> logger)
        {
            if (logger == null)
                throw new Exception("Empty handler");

            _logger = logger;
            _config = config;

            RecordSchema = (RecordSchema)RecordSchema.Parse(GetSchemaAsString(schemaLocation));

            _topic = (string)config[KafkaPropNames.Topic];

            _schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                SchemaRegistryUrl = (string)_config[KafkaPropNames.SchemaRegistryUrl],
                SchemaRegistryRequestTimeoutMs = (int)_config[KafkaPropNames.SchemaRegistryRequestTimeoutMs],
            });
        }

        private IProducer<string, GenericRecord> CreateProducer() =>
            new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = (string)_config[KafkaPropNames.BootstrapServers] })
                    .SetKeySerializer(new AvroSerializer<string>(_schemaRegistry))
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(_schemaRegistry))
                    .Build();

        #endregion // Ctor & Create producer

        #region Send 

        public async Task SendAsync(string key, GenericRecord value)
        {
            if (_producer == null || ShouldProducerReset)
                _producer = CreateProducer();

            await _producer.ProduceAsync(_topic, new Message<string, GenericRecord> 
                                    { Key = key, Value = value })
                           .ContinueWith(task =>
                            {
                                if (task.IsFaulted)
                                    _logger(task.Exception.Message);
                            });
        }

        #endregion // Send

        #region Dispose 

        private bool ShouldProducerReset => DateTime.Now - _creationTime > MaxLiveTimeSpan;

        public void Dispose()
        {
            _quePair = null;
            _producer?.Dispose();
            _logger("\nProducer was stopped.");
        }

        #endregion // Dispose 

        private static string GetSchemaAsString(string schemaLocation)
        {
            try
            {
                return File.ReadAllText(schemaLocation);
            }
            catch
            {
                try
                {
                    return new HttpClient().GetStringAsync(schemaLocation).Result;
                }
                catch (Exception e)
                {
                    Console.WriteLine($"ERROR: Schema string was not obtained from \"{schemaLocation}\". {e}");
                    return null;
                }
            }
        }
    }
}

