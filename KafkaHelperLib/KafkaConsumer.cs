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
    public class KafkaConsumer
    {
        #region Vars

        private IConsumer<string, GenericRecord> _consumer;
        private CancellationTokenSource _cts;
        private Action<string, GenericRecord, DateTime> _consumeResultHandler;
        private Action<string> _errorHandler;
        private string _topic;
        private Task _taskConsumer;

        public RecordConfig GenericRecordConfig { get; }
        public RecordSchema RecordSchema { get; }

        #endregion // Vars

        #region Ctor

        public KafkaConsumer(Dictionary<string, object> config,
                             Action<string, dynamic, DateTime> consumeResultHandler,
                             Action<string> errorHandler)
        {
            if (consumeResultHandler == null || errorHandler == null)
                throw new Exception("Empty handler");

            _consumeResultHandler = consumeResultHandler;
            _errorHandler = errorHandler;

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
                    .SetErrorHandler((_, e) => errorHandler(e.Reason))
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
                    var cr = await Task<ConsumeResult<string, byte[]>>.Run(() =>
                    {
                        try
                        {
                            return _consumer.Consume(_cts.Token);
                        }
                        catch (Exception e)
                        {
                            _errorHandler(e.Message);
                        }

                        return null;
                    });
                    
                    if (cr != null)
                        _consumeResultHandler(cr.Key, cr.Value, cr.Timestamp.UtcDateTime);
                }
            }
            catch (Exception e)
            {
                _errorHandler(e.Message);
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
            _taskConsumer?.Wait();
        }

        #endregion // Dispose
    }
}
