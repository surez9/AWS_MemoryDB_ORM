using Newtonsoft.Json;
using Redis.OM.Modeling;

namespace ESDMemoryTest.Model
{
    [Document(IndexName ="idx:devicesession", StorageType = StorageType.Json)]
    //    [Index("idx:devicesession")] // Specifies the index name
    public class DeviceSession
    {
        // [Indexed] // Ensure this property is indexed
        [Indexed]
        [JsonProperty(PropertyName = "purl")]
        public string PURL { get; set; }

        [Indexed]
        [RedisIdField]// Make the Id indexed for quick lookups
        [JsonProperty(PropertyName = "Id")]
        public Guid Id { get; set; }

        [Indexed]
        [JsonProperty(PropertyName = "startUtc")]
        public DateTime StartUtc { get; set; }

        [Indexed]
        [JsonProperty(PropertyName = "expireUtc")]
        public DateTime ExpireUtc { get; set; }

        [Indexed]
        [JsonProperty(PropertyName = "lastModifiedUtc")]
        public DateTime LastModifiedUtc { get; set; }

        [Indexed]
        [JsonProperty(PropertyName = "lockedOutUntilUtc")]
        public DateTime? LockedOutUntilUtc { get; set; }

        [Searchable]
        [JsonProperty(PropertyName = "token")]
        public string? Token { get; set; }

        [Searchable]
        [JsonProperty(PropertyName = "session")]
        public string? Session { get; set; }
    }
}
