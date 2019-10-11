using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaHelperLib
{
    public class KafkaConsumer : IDisposable
    {
        #region Vars

        private IConsumer<string, GenericRecord> _consumer;
        private CancellationTokenSource _cts;
        private Action<string, GenericRecord, DateTime> _consumeResultHandler;
        private Action<string> _logger;
        private string _topic;
        private Task _taskConsumer;

        public RecordConfig GenericRecordConfig { get; }
        public RecordSchema RecordSchema { get; }

        #endregion // Vars

        #region Ctor

        public KafkaConsumer(Dictionary<string, object> config,
                             Action<string, dynamic, DateTime> consumeResultHandler,
                             Action<string> logger)
        {
            if (consumeResultHandler == null || logger == null)
                throw new Exception("Empty handler");

            _consumeResultHandler = consumeResultHandler;
            _logger = logger;

            _cts = new CancellationTokenSource();

            var genericRecordConfig = new RecordConfig((string)config[KafkaPropNames.SchemaRegistryUrl]);
            RecordSchema = genericRecordConfig.RecordSchema;

            _consumer = new ConsumerBuilder<string, GenericRecord>(
                    new ConsumerConfig
                    {
                        BootstrapServers = (string)config[KafkaPropNames.BootstrapServers],
                        GroupId = (string)config[KafkaPropNames.GroupId],
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    })
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(new AvroDeserializer<GenericRecord>(genericRecordConfig.GetSchemaRegistryClient()).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => logger(e.Reason))
                    .Build();

            _topic = (string)config[KafkaPropNames.Topic];

            _consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(_topic, (int)config[KafkaPropNames.Partition], (int)config[KafkaPropNames.Offset]) });
        }

        #endregion // Ctor

        #region StartConsuming Method

        public KafkaConsumer StartConsuming() 
        {
            _taskConsumer = StartConsumingInner();
            return this;
        }

        private async Task StartConsumingInner()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var cr = await Task<ConsumeResult<string, GenericRecord>>.Run(() =>
                    {
                        try
                        {
                            return _consumer.Consume(_cts.Token);
                        }
                        catch (Exception e)
                        {
                            _logger(e.Message);
                        }

                        return null;
                    });
                    
                    if (cr != null)
                        _consumeResultHandler(cr.Key, cr.Value, cr.Timestamp.UtcDateTime);
                }
            }
            catch (Exception e)
            {
                _logger(e.Message);
            }
            finally
            {
                _consumer.Close();
            }
        }

        #endregion // StartConsuming Method

        #region Dispose 

        public void Dispose()
        {
            _cts?.Cancel();
            _logger("\nConsumer was stopped.");
            _taskConsumer?.Wait();
        }

        #endregion // Dispose
    }
}
