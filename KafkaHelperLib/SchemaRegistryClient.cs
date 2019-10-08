using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.SchemaRegistry;

namespace KafkaHelperLib
{
    public class SchemaRegistryClient : ISchemaRegistryClient
    {
        private Schema _schema;

        public int MaxCachedSchemas { get; } = 1;

        public SchemaRegistryClient(Schema schema)
        {
            if (schema == null)
                throw new Exception("Null Schema");

            _schema = schema;
        }

        public string ConstructKeySubjectName(string topic) => _schema.Subject;

        public string ConstructValueSubjectName(string topic) => _schema.Subject;

        public Task<List<string>> GetAllSubjectsAsync() => Task.Run(() => new List<string>());

        public Task<Schema> GetLatestSchemaAsync(string subject) => Task.Run(() => _schema);

        public Task<string> GetSchemaAsync(int id) => Task.Run(() => _schema.SchemaString);

        public Task<string> GetSchemaAsync(string subject, int version) => Task.Run(() => _schema.SchemaString);

        public Task<int> GetSchemaIdAsync(string subject, string schema) => Task.Run(() => _schema.Id);

        public Task<List<int>> GetSubjectVersionsAsync(string subject) => Task.Run(() => new List<int> { _schema.Version });

        public Task<bool> IsCompatibleAsync(string subject, string schema) => Task.Run(() => true);

        public Task<int> RegisterSchemaAsync(string subject, string schema) => Task.Run(() => _schema.Id);

        public void Dispose()
        {
            _schema = null;
        }
    }
}

