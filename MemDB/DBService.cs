using ESDMemoryTest.Model;
using Redis.OM.Searching;
using Redis.OM;
using Amazon.MemoryDB;
using StackExchange.Redis;
using System.Linq;
using NRedisStack.RedisStackCommands;
using Eliza.Common.AmazonWebServicesV2.SQS;
using System.Collections.Generic;

namespace ESDMemoryORMTest.MemDB
{
    public class DBService
    {
        private readonly RedisCollection<DeviceSession> redisDB;
        private readonly RedisConnectionProvider _provider;
        private readonly IConnectionMultiplexer _multiplexer;

        public DBService(RedisConnectionProvider provider, IConnectionMultiplexer multiplexer)
        {
            _provider = provider;
            _multiplexer = multiplexer;
             redisDB = (RedisCollection<DeviceSession>)_provider.RedisCollection<DeviceSession>();
        }




        // Method to insert a batch DeviceSession
        public async Task CreateBatchRecordAsync()
        {
            int count = 100000;
            string session = "{\"DataRecordId\":-6964772598236045992,\"RequestId\":\"string\",\"MemberNumber\":\"ElizaSQA-AIW-20240902092020-207-0000000001\",\"ResultId\":\"string\",\"JobId\":\"1779605070374970485\",\"CustomerName\":\"SQA-Controlled-USA\",\"ProgramName\":\"SQA_ESD\",\"ApplicationName\":\"SQA_SD\",\"SourceApplication\":\"SQA_SD_EMAIL\",\"ResultDateTime\":\"2024-09-05T09:48:10.926Z\",\"ResultDuration\":0,\"Channel\":\"WebSurvey\",\"Result\":\"string\",\"ResultReason\":\"string\",\"ResultInformation\":{\"SQ_ZIPCODE\":\"56356\",\"SQ_OCPN\":\"4172111111\"}}";
            string token = "eyJraWQiOiJqN3ZscEt2TlluMHByeU1CNnp3ZGt2MHo3cHA4dkYwTm1nVDVpYUR4bWU0PSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiI0ZzB1ZXZpMXVuc3UwaWRva2ZjMTN0MXB0MyIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiRWxpemFTZWN1cmVEaWdpdGFsXC9TZXNzaW9uQVBJIEVsaXphU2VjdXJlRGlnaXRhbFwvVmVyaWZpY2F0aW9uQVBJIiwiYXV0aF90aW1lIjoxNzI3MTg5OTkyLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0xLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMV8wUTFOd0docUkiLCJleHAiOjE3MjcxOTE3OTIsImlhdCI6MTcyNzE4OTk5MiwidmVyc2lvbiI6MiwianRpIjoiOWMyMjljMWItZmIyZS00ZTgxLWJkMDctYmQ3OTc2NDc1MTU3IiwiY2xpZW50X2lkIjoiNGcwdWV2aTF1bnN1MGlkb2tmYzEzdDFwdDMifQ.NeH8DYqLMTiLFYZEWaA1m2IGKEUXvmrIZNSXG-TWUtSQ5KQDjeOYYLMrExLf8GxMk1oXVxGTz714rqo2t639X8JY_SdRAm3-wAGZZhYUwXkNkNQWZMYA9XtfP_EgDkoEagiksxm8LbUnEkpla-SbvosavP3I3lHrAO3I38ltOyayfmT4Di2HTk95uguO9S-emX0CY6Qehzi4z_y4VhSIp4I2mMziDYX-GuwOukqOOnIaAvddfcAzyXzFZKQh_gkHunkU9xs9eEeAXT8nj9U9suvd-YOHcqGsS_Yc5OjjB4kGfAl3m-2jVD4xZ22ki2TVkwPsEfXQqHZnQuiRW8V1zw";
            for (int i = 0; i < count; i++)
            {
                DeviceSession deviceSession = new DeviceSession();
                deviceSession.Id = Guid.NewGuid();
                deviceSession.PURL = $"0fdsaj9d:{i}";
                deviceSession.StartUtc = DateTime.Now;
                deviceSession.LockedOutUntilUtc = DateTime.Now;
                deviceSession.LastModifiedUtc = DateTime.Now;
                deviceSession.ExpireUtc = DateTime.Now.AddHours(-1);
                deviceSession.Session = session;
                deviceSession.Token = token;

                await redisDB.InsertAsync(deviceSession);
               
            }
           
        }

        
        // Complete Method to Get all the expired record in limited number per loop
        // ..adding in batch
        // ....queueing the record in SQS and deleting in batch
        
        //Queue the record one by one
        public async Task<bool> GetSessionByBatchAndDelete()
        {
            int recordSize = 10000;  // number of record fetched per loop
            var now = DateTime.UtcNow;
            var allExpiredRecords = new List<DeviceSession>();

            int offset = 0; // Start from the beginning, need to keep track of how many records have been fetched so far.

            //Getting database instance
            var db = _multiplexer.GetDatabase();

            List<DeviceSession> batch;

            do
            {
                // Fetch a batch of expired records
                batch = (List<DeviceSession>)await redisDB
                    .Where(ds => ds.ExpireUtc < now)
                    .Skip(offset)  // Skipping previously fetched records
                    .Take(recordSize)
                    .ToListAsync();

                // If we have records, add them to the cumulative list
                if (batch.Count > 0)
                {
                    allExpiredRecords.AddRange(batch);
                    offset += batch.Count; // Move the offset forward
                }
            } while (batch.Count == recordSize); // Continue if we got a full batch

            // created to execute batch delete
            var batchDelete = db.CreateBatch();

            //queuing the session in sqs
            AwsSqs awsSqs = new AwsSqs();

            foreach (var record in allExpiredRecords)
            {
                string key = "ESDMemoryTest.Model.DeviceSession:" + record.Id;

                string queueUrl = "https://sqs.us-east-1.amazonaws.com/066764241246/ESDMemDBORM";
                awsSqs.PushMessage(queueUrl, record.Session);
                
                batchDelete.KeyDeleteAsync(key);

            }

            batchDelete.Execute();

            return true;
        }


        // Complete Method to Get all the expired record in limited number per loop
        // adding in batch
        // queueing the record in SQS and deleting in batch
        public async Task<bool> GetSessionBatchQueuing()
        {
            int recordSize = 10000;  // number of record fetched per loop
            var now = DateTime.UtcNow;
            var allExpiredRecords = new List<DeviceSession>();

            int offset = 0; // Start from the beginning, need to keep track of how many records have been fetched so far.

            //Getting database instance
            var db = _multiplexer.GetDatabase();

            List<DeviceSession> batch;

            do
            {
                // Fetch a batch of expired records
                batch = (List<DeviceSession>)await redisDB
                    .Where(ds => ds.ExpireUtc < now)
                    .Skip(offset)  // Skipping previously fetched records
                    .Take(recordSize)
                    .ToListAsync();

                // If we have records, add them to the cumulative list
                if (batch.Count > 0)
                {
                    allExpiredRecords.AddRange(batch);
                    offset += batch.Count; // Move the offset forward
                }
            } while (batch.Count == recordSize); // Continue if we got a full batch

            //queuing the session in sqs
            AwsSqs awsSqs = new AwsSqs();
            ICollection<string> messages = new List<string>();
            
            foreach (var record in allExpiredRecords)
            {
                string key = "ESDMemoryTest.Model.DeviceSession:" + record.Id;

                messages.Add(record.Session);

                //Redis handle record expiry automatically
                db.KeyExpireAsync(key, TimeSpan.FromMinutes(2));

            }
            string queueUrl = "https://sqs.us-east-1.amazonaws.com/066764241246/ESDMemDBORM";
            awsSqs.PushMessages(queueUrl,messages ,30);

            return true;
        }




        // Method to get all sessions with query
        public async Task<IEnumerable<DeviceSession>> GetByQuery()
        {
            var db = _multiplexer.GetDatabase();
            var result = db.Execute("FT.SEARCH", "idx:devicesession", "*");

            return await redisDB.ToListAsync();
        }


        // Method to create a new DeviceSession
        public async Task CreateAsync(DeviceSession deviceSession)
        {
            if (deviceSession == null) throw new ArgumentNullException(nameof(deviceSession));

            deviceSession.Id = Guid.NewGuid();
            await redisDB.InsertAsync(deviceSession);
        }

        // Method to retrieve a DeviceSession by ID
        public async Task<DeviceSession> GetByIdAsync(Guid id)
        {
            var ds = await redisDB.Where(ds => ds.Id == id).FirstOrDefaultAsync();
            return ds;
        }

        public DeviceSession GetById(String id)
        {
            // Use the synchronous equivalent of the query\
            Guid ID = new Guid(id);
            var ds = redisDB.Where(ds => ds.Id == ID).FirstOrDefault(); // Ensure you have a sync method
            return ds; // This will be null if no matching record is found
        }

        // New method to get sessions by Purl
        public DeviceSession GetByPurl(string purl)
        {
            if (string.IsNullOrEmpty(purl)) return null;
            var ds =   redisDB.Where(ds => ds.PURL == purl).FirstOrDefault();
            return ds;
        }

        // Method to get all sessions
        public async Task<IEnumerable<DeviceSession>> GetAllSessionsAsync()
        {
            return await redisDB.ToListAsync(); 
        }

      


        //update expiry in the memoryDB before deleting.....
        public async Task<bool> UpdateAsync(Guid id, DeviceSession updatedSession)
        {
            if (updatedSession == null) throw new ArgumentNullException(nameof(updatedSession));
            var existingSession = await GetByIdAsync(id);
            if (existingSession == null) return false;

            // Update existing session's properties
            existingSession.ExpireUtc = updatedSession.ExpireUtc;
            existingSession.LastModifiedUtc = DateTime.UtcNow;
            existingSession.LockedOutUntilUtc = updatedSession.LockedOutUntilUtc;
            existingSession.PURL = updatedSession.PURL;
            existingSession.Session = updatedSession.Session;
            existingSession.StartUtc = updatedSession.StartUtc;
            existingSession.Token = updatedSession.Token;

            await redisDB.InsertAsync(existingSession);
            return true;
        }

        public async Task<bool> DeleteAsync(Guid id)
        {
            var existingSession = await GetByIdAsync(id);
            if (existingSession == null) return false;

            await redisDB.DeleteAsync(existingSession); 
            return true; // Successfully deleted
        }

 

        // New method to fetch expired sessions
        public async Task<IEnumerable<DeviceSession>> GetExpiredSessionsAsync() 
        { 
            var now = DateTime.UtcNow;  
            return await redisDB.Where(ds => ds.ExpireUtc < now).ToListAsync();
        } 

        // New method to delete expired sessions
        public async Task<IEnumerable<DeviceSession>> DeleteExpiredSessionsAsync()
        {
            var now = DateTime.UtcNow;
            var allExpiredRecords = await redisDB.Where(ds => ds.ExpireUtc < now).ToListAsync();

            // Add to queue ---{}
 
            foreach (var record in allExpiredRecords) 
            { 
               await redisDB.DeleteAsync(record);   
 
            }  
            return null;  
        }


        // Getting device session with the range of start UTC
        public async Task<IEnumerable<DeviceSession>> GetSessionsInRangeAsync()
        {
            DateTime startTime = DateTime.Parse("2024-09-24T08:37:18.946+00:00");
            DateTime endTime = DateTime.Parse("2024-09-24T08:37:19.946+00:00");
            var allSessions = await redisDB.Where(ds => ds.StartUtc >= startTime && ds.StartUtc <= endTime).ToListAsync();
            return allSessions;
        }

        // Getting device session with the range of start UTC
        public async Task<IEnumerable<DeviceSession>> GetSessionsForLastTwoHours()
        {
            DateTime startTime = DateTime.Now.AddHours(-2);
            DateTime endTime = DateTime.Now;
            var allSessions = await redisDB.Where(ds => ds.StartUtc >= startTime && ds.StartUtc <= endTime).ToListAsync();
            return allSessions;

        }

            public async Task<List<DeviceSession>> GetAllExpiredSessionsByBatchAsync()
            {
                int batchSize = 10000;
                var now = DateTime.UtcNow;
                var allExpiredRecords = new List<DeviceSession>();

                int offset = 0; // Start from the beginning, need to keep track of how many records have been fetched so far.

                List<DeviceSession> batch;
                do
                {
                    // Fetch a batch of expired records
                    batch = (List<DeviceSession>)await redisDB
                        .Where(ds => ds.ExpireUtc < now)
                        .Skip(offset)  // Skipping previously fetched records
                        .Take(batchSize)
                        .ToListAsync();

                    // If we have records, add them to the cumulative list
                    if (batch.Count > 0)
                    {
                        allExpiredRecords.AddRange(batch);
                        offset += batch.Count; // Move the offset forward
                    }
                } while (batch.Count == batchSize); // Continue if we got a full batch

                return allExpiredRecords;
            }

        }


        // New method to search sessions by text field
        public async Task<IEnumerable<DeviceSession>> SearchBySessionTextAsync(string session)
        {
            if (string.IsNullOrEmpty(session)) return Array.Empty<DeviceSession>();

            var result=  await redisDB.Where(ds => ds.Session == session).ToListAsync();
            return result;
        }

        // New method to get sessions by Purl
        public async Task<IEnumerable<DeviceSession>> GetByPurlAsync(string purl)
        {
            if (string.IsNullOrEmpty(purl)) return Array.Empty<DeviceSession>();
            return await redisDB.Where(ds => ds.PURL == purl).ToListAsync();
        }

    }
}
