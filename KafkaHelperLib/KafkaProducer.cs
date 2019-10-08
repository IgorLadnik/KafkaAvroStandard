using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using System.Collections.Concurrent;
using Avro.Generic;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace KafkaHelperLib
{
    public class KafkaProducer
    {
        #region Vars

        private ConcurrentQueue<KeyValuePair<string, GenericRecord>> _cquePair = new ConcurrentQueue<KeyValuePair<string, GenericRecord>>();

        private AvroSerializer<GenericRecord> _avroSerializer;
        private IProducer<string, byte[]> _producer;
        private Task _task;
        private bool _isClosing = false;
        private string _topic;
        private long _isSending = 0;

        public RecordConfig GenericRecordConfig { get; }
        public string Error { get; private set; }

        #endregion // Vars

        #region Ctor
                    
        public KafkaProducer(Dictionary<string, object> config,
                             Action<string> errorHandler)
        {
            if (errorHandler == null)
                throw new Exception("Empty handler");

            //1 var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            //{
            //    SchemaRegistryUrl = schemaRegistryUrl,
            //    SchemaRegistryRequestTimeoutMs = 5000,
            //});
            //var schemaRegistry = new SchemaRegistryClient(new Schema(recordConfig.Subject, recordConfig.Version, recordConfig.Id, recordConfig.SchemaString)); //1
            
            GenericRecordConfig = new RecordConfig((string)config[KafkaPropNames.SchemaRegistryUrl]);
            var schema = new Schema(GenericRecordConfig.Subject, GenericRecordConfig.Version, GenericRecordConfig.Id, GenericRecordConfig.SchemaString);
            var schemaRegistry = new SchemaRegistryClient(schema);
            _avroSerializer = new AvroSerializer<GenericRecord>(schemaRegistry);

            _producer =
                new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = (string)config[KafkaPropNames.BootstrapServers] })
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(Serializers.ByteArray/*new AvroSerializer<T>(schemaRegistry)*/)
                    .Build();

            _topic = (string)config[KafkaPropNames.Topic];
        }

        #endregion // Ctor

        #region Serialize

        public async Task<byte[]> SerializeAsync(GenericRecord genericRecord, string topic) =>
            await _avroSerializer.SerializeAsync(genericRecord, new SerializationContext(MessageComponentType.Value, topic));

        #endregion // Serialize

        #region Send Methods 

        public KafkaProducer Send(string key, GenericRecord value)
        {
            if (string.IsNullOrEmpty(key) || value == null)
                return this;

            return Send(new KeyValuePair<string, GenericRecord>(key, value));
        }

        public KafkaProducer Send(List<Tuple<string, GenericRecord>> lst) => Send(lst?.ToArray());

        public KafkaProducer Send(params Tuple<string, GenericRecord>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;

            var lst = new List<KeyValuePair<string, GenericRecord>>();
            foreach (var tuple in arr)
                lst.Add(new KeyValuePair<string, GenericRecord>(tuple.Item1, tuple.Item2));

            return Send(lst.ToArray());
        }

        public KafkaProducer Send(params KeyValuePair<string, GenericRecord>[] arr)
        {
            if (arr == null || arr.Length == 0)
                return this;

            if (!_isClosing)
                foreach (var pair in arr)
                    _cquePair.Enqueue(new KeyValuePair<string, GenericRecord>(pair.Key, pair.Value));

            if (Interlocked.Read(ref _isSending) == 1)
                return this;

            _task = Task.Run(async () =>
            {
                DeliveryResult<string, byte[]> dr = null;

                Interlocked.Increment(ref _isSending);

                while (_cquePair.TryDequeue(out var pair))
                {
                    try
                    {
                        dr = await _producer.ProduceAsync(_topic, new Message<string, byte[]> { Key = pair.Key, Value = await SerializeAsync(pair.Value, _topic) });
                        //.ContinueWith(task => task.IsFaulted
                        //         ? $"error producing message: {task.Exception.Message}"
                        //         : $"produced to: {task.Result.TopicPartitionOffset}")
                        //.Wait();
                    }
                    catch (Exception e)
                    {
                        //Console.WriteLine($"Send failed: {e}");
                        Error = e.Message;
                        break;
                    }
                }

                Interlocked.Decrement(ref _isSending);
            });

            return this;
        }

        #endregion // Send Methods

        #region Dispose 

        public void Dispose()
        {
            _isClosing = true;
            _cquePair = null;

            _task?.Wait();

            _producer?.Dispose();
        }

        #endregion // Dispose 
    }
}

