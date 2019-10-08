using System;
using System.Collections.Generic;
using System.Threading;
using Confluent.Kafka;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using System.Threading.Tasks;

namespace KafkaHelperLib
{
    public class KafkaConsumer
    {
        #region Vars

        private IConsumer<string, byte[]> _consumer;
        private CancellationTokenSource _cts;
        private AvroDeserializer<GenericRecord> _avroDeserializer;
        private Action<string, GenericRecord, DateTime> _consumeResultHandler;
        private Action<string> _errorHandler;
        private string _topic;
        private Task _taskConsumer;

        public RecordConfig GenericRecordConfig { get; }

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

            GenericRecordConfig = new RecordConfig((string)config[KafkaPropNames.SchemaRegistryUrl]);
            var schema = new Schema(GenericRecordConfig.Subject, GenericRecordConfig.Version, GenericRecordConfig.Id, GenericRecordConfig.SchemaString);
            //1 var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = schemaRegistryUrl });
            var schemaRegistry = new SchemaRegistryClient(schema);
            _avroDeserializer = new AvroDeserializer<GenericRecord>(schemaRegistry);

            _consumer = new ConsumerBuilder<string, byte[]>(
                    new ConsumerConfig
                    {
                        BootstrapServers = (string)config[KafkaPropNames.BootstrapServers],
                        GroupId = (string)config[KafkaPropNames.GroupId],
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    })
                    .SetKeyDeserializer(Deserializers.Utf8)
                    .SetValueDeserializer(Deserializers.ByteArray/*new AvroDeserializer<T>(schemaRegistry).AsSyncOverAsync()*/)
                    .SetErrorHandler((_, e) => errorHandler(e.Reason))
                    .Build();

            _topic = (string)config[KafkaPropNames.Topic];

            _consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(_topic, (int)config[KafkaPropNames.Partition], (int)config[KafkaPropNames.Offset]) });
        }

        #endregion // Ctor

        #region Deserialize

        public async Task<GenericRecord> DeserializeAsync(byte[] bts, string topic) =>
            await _avroDeserializer.DeserializeAsync(bts, false, new SerializationContext(MessageComponentType.Value, topic));

        #endregion // Deserialize

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
                        _consumeResultHandler(cr.Key, await DeserializeAsync(cr.Value, _topic), cr.Timestamp.UtcDateTime);
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
