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
        #region ReadOnly 

        private readonly TimeSpan MaxLiveTimeSpan = TimeSpan.FromMinutes(3);

        #endregion // ReadOnly

        #region Vars

        private CancellationTokenSource _cts;
        private Action<string, GenericRecord, DateTime> _consumeResultHandler;
        private Action<string> _logger;
        private string _topic;
        private Task _taskConsumer;
        private Dictionary<string, object> _config;
        private RecordConfig _genericRecordConfig;
        private DateTime _creationTime;

        public RecordConfig GenericRecordConfig { get; }
        public RecordSchema RecordSchema { get; }

        #endregion // Vars

        #region Ctor & Consumer Creation

        public KafkaConsumer(Dictionary<string, object> config,
                             Action<string, dynamic, DateTime> consumeResultHandler,
                             Action<string> logger)
        {
            if (consumeResultHandler == null || logger == null)
                throw new Exception("Empty handler");

            _consumeResultHandler = consumeResultHandler;
            _logger = logger;
            _config = config;

            _cts = new CancellationTokenSource();

            _genericRecordConfig = new RecordConfig((string)config[KafkaPropNames.SchemaRegistryUrl]);
            RecordSchema = _genericRecordConfig.RecordSchema;

            _topic = (string)config[KafkaPropNames.Topic];

            _taskConsumer = StartConsumingInner();
        }

        private IConsumer<string, GenericRecord> CreateConsumer()
        {
            var schemaRegistry = _genericRecordConfig.GetSchemaRegistryClient();
            var consumer = new ConsumerBuilder<string, GenericRecord>(
                    new ConsumerConfig
                    {
                        BootstrapServers = (string)_config[KafkaPropNames.BootstrapServers],
                        GroupId = (string)_config[KafkaPropNames.GroupId],
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    })
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => _logger(e.Reason))
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Stats: {json}"))
                    .Build();

            consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(_topic, (int)_config[KafkaPropNames.Partition], (int)_config[KafkaPropNames.Offset]) });

            _creationTime = DateTime.Now;

            return consumer;
        }

        #endregion // Ctor & Consumer Creation

        #region StartConsuming Method

        private async Task StartConsumingInner()
        {
            IConsumer<string, GenericRecord> consumer = null;

            while (!_cts.IsCancellationRequested)
            {
                if (consumer == null || ShouldConsumerReset)
                {
                    try { consumer?.Close(); } catch (Exception) { };
                    consumer = CreateConsumer();
                }

                var cr = await Task<ConsumeResult<string, GenericRecord>>.Run(() =>
                {
                    try
                    {
                        return consumer.Consume(_cts.Token);                       
                    }
                    catch (OperationCanceledException) 
                    {
                        _logger("Consumer: Operation cancelled.");
                        consumer.Close();
                    }
                    catch (Exception e)
                    {
                        _logger(e.Message);
                        try { consumer.Close(); } catch (Exception) { };
                        consumer = null;
                    }

                    return null;
                });
                    
                if (cr != null)
                    _consumeResultHandler(cr.Key, cr.Value, cr.Timestamp.UtcDateTime);
            }
        }

        #endregion // StartConsuming Method

        #region Reset condition & Dispose 

        private bool ShouldConsumerReset => DateTime.Now - _creationTime > MaxLiveTimeSpan;

        public void Dispose()
        {
            _cts?.Cancel();
            _logger("\nConsumer was stopped.");
            _taskConsumer?.Wait();
        }

        #endregion // Reset condition & Dispose
    }
}
