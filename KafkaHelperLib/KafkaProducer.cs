﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
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
        RecordConfig _genericRecordConfig;
        private DateTime _creationTime;
        
        private Action<string> _logger;

        public RecordSchema RecordSchema { get; private set; }

        #endregion // Vars

        #region Ctor & Create producer

        public KafkaProducer(Dictionary<string, object> config,
                             Action<string> logger)
        {
            if (logger == null)
                throw new Exception("Empty handler");

            _logger = logger;
            _config = config;

            _genericRecordConfig = new RecordConfig((string)_config[KafkaPropNames.SchemaRegistryUrl]);
            RecordSchema = _genericRecordConfig.RecordSchema;

            _topic = (string)config[KafkaPropNames.Topic];
        }

        private IProducer<string, GenericRecord> CreateProducer() =>
            new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = (string)_config[KafkaPropNames.BootstrapServers] })
                        .SetKeySerializer(Serializers.Utf8)
                        .SetValueSerializer(new AvroSerializer<GenericRecord>(_genericRecordConfig.GetSchemaRegistryClient()))
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
    }
}

