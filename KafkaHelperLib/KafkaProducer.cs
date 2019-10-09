using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaHelperLib
{
    public class KafkaProducer
    {
        #region Vars

        private ConcurrentQueue<KeyValuePair<string, GenericRecord>> _cquePair = new ConcurrentQueue<KeyValuePair<string, GenericRecord>>();

        private IProducer<string, GenericRecord> _producer;
        private Task _task;
        private bool _isClosing = false;
        private string _topic;
        private long _isSending = 0;

        private Action<string> _errorHandler;

        public RecordSchema RecordSchema { get; }

        #endregion // Vars

        #region Ctor

        public KafkaProducer(Dictionary<string, object> config,
                             Action<string> errorHandler)
        {
            if (errorHandler == null)
                throw new Exception("Empty handler");

            _errorHandler = errorHandler;

            var genericRecordConfig = new RecordConfig((string)config[KafkaPropNames.SchemaRegistryUrl]);
            RecordSchema = genericRecordConfig.RecordSchema;

            _producer =
                new ProducerBuilder<string, GenericRecord>(new ProducerConfig { BootstrapServers = (string)config[KafkaPropNames.BootstrapServers] })
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(genericRecordConfig.GetSchemaRegistryClient()))
                    .Build();

            _topic = (string)config[KafkaPropNames.Topic];
        }

        #endregion // Ctor

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
                DeliveryResult<string, GenericRecord> dr = null;

                Interlocked.Increment(ref _isSending);

                while (_cquePair.TryDequeue(out var pair))
                {
                    try
                    {
                        dr = await _producer.ProduceAsync(_topic, new Message<string, GenericRecord> { Key = pair.Key, Value = pair.Value });
                        //.ContinueWith(task => task.IsFaulted
                        //         ? $"error producing message: {task.Exception.Message}"
                        //         : $"produced to: {task.Result.TopicPartitionOffset}")
                        //.Wait();
                    }
                    catch (Exception e)
                    {
                        _errorHandler(e.Message);
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

