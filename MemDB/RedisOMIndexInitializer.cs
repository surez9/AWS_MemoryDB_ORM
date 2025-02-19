using ESDMemoryTest.Model;
using Redis.OM;
using System;

namespace ESDMemoryTest.MemDB
{
    public class RedisOMIndexInitializer : IHostedService
    {
        private readonly RedisConnectionProvider _redisConnectionProvider;

        public RedisOMIndexInitializer(RedisConnectionProvider redisConnectionProvider)
        {
            _redisConnectionProvider = redisConnectionProvider;
        }

        /// <summary>
        /// Checks redis to see if the index already exists, if it doesn't create a new index
        /// </summary>
        /// <param name="cancellationToken"></param>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var info = (await _redisConnectionProvider.Connection.ExecuteAsync("FT._LIST")).ToArray().Select(x => x.ToString());
            if (info.All(x => x != "idx:devicesession"))
            {
                await _redisConnectionProvider.Connection.CreateIndexAsync(typeof(DeviceSession));
            }
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
