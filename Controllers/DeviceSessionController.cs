using Microsoft.AspNetCore.Mvc;
using ESDMemoryTest.MemDB;
using Microsoft.AspNetCore.Authorization;
using Amazon.Lambda.Core;
using ESDMemoryTest.Model;
using Newtonsoft.Json;
using ESDMemoryORMTest.MemDB;

namespace ESDMemoryTest.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [AllowAnonymous]
    public class DeviceSessionController : Controller
    {
        private readonly DBService _dbService;

        public DeviceSessionController(DBService dbService)
        {
            _dbService = dbService;
        }

        [HttpGet("batchAdd")]
        public async Task<IActionResult> AddBatchRecord()
        {
            await _dbService.CreateBatchRecordAsync();
            return Ok("Done");

        }

        [HttpPost("addDeviceSession")]
        public async Task<IActionResult> AddDeviceSession([FromBody] DeviceSession deviceSession)
        {
            if (deviceSession == null)
            {
                return BadRequest("Device session cannot be null.");
            }
            
            await _dbService.CreateAsync(deviceSession);
            return CreatedAtAction(nameof(GetById), new { id = deviceSession.Id }, deviceSession);

        }
        // GET api/devicesession/{id}
        [HttpGet]
        public IActionResult GetById([FromQuery] string id)
        {
            var deviceSession =  _dbService.GetById(id);

            if (deviceSession == null)
            {
                return NotFound(); 
            }

            return Ok(deviceSession); 
        }

        //Get all the session
        [HttpGet("all")]
        public async Task<IActionResult> GetAllSessions()
        {
            var sessions = await _dbService.GetAllSessionsAsync();
            return Ok(sessions); // Return 200 with the list of sessions
        }

        //Get all session by query
        [HttpGet("all/query")]
        public async Task<IActionResult> GetAllSessionByQuery()
        {
            var sessions = await _dbService.GetByQuery();
            return Ok(sessions); // Return 200 with the list of sessions
        }

        //Get session with time range
        [HttpGet("session/range")]
        public async Task<IActionResult> GetRangeSessions()
        {
            var sessions = await _dbService.GetSessionsInRangeAsync();
            return Ok(sessions); // Return 200 with the list of sessions
        }

        //Get session created from last two hours
        [HttpGet("session/lastTwoHours")]
        public async Task<IActionResult> GetLastHoursSessions()
        {
            var sessions = await _dbService.GetSessionsForLastTwoHours();
            return Ok(sessions); // Return 200 with the list of sessions
        }

        //Update the session
        [HttpPut("{id}")]
        public async Task<IActionResult> Update(Guid id, [FromBody] DeviceSession updatedSession)
        {
            if (updatedSession == null)
            {
                return BadRequest("Updated session cannot be null.");
            }
            bool isUpdated = await _dbService.UpdateAsync(id, updatedSession);
            if (!isUpdated)
            {
                return NotFound(); 
            }

            return Ok("Updated"); 
        }

        // DELETE api/devicesession/{id}
        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(Guid id)
        {
            bool isDeleted = await _dbService.DeleteAsync(id);
            if (!isDeleted)
            {
                return NotFound();
            }

            return Ok("Deleted");
        }

        // GET api/devicesession/expired
        [HttpGet("expired")]
        public async Task<IActionResult> GetExpiredSessions()
        {
            var expiredSessions = await _dbService.GetExpiredSessionsAsync();
            return Ok(expiredSessions);
        }


        // GET api/devicesession/expired
        [HttpGet("expired/batch")]
        public async Task<IActionResult> GetExpiredSessionsBatch()
        {
            var expiredSessions = await _dbService.GetAllExpiredSessionsByBatchAsync();
            return Ok(expiredSessions);
        }


        //Queue the record one by one
        [HttpGet("expired/batchDelete")]
        public async Task<IActionResult> GetExpiredSessionsBatchDelete()
        {
            var expiredSessions = await _dbService.GetSessionByBatchAndDelete();
            return Ok(expiredSessions);
        }


        //Queue the record in batch
        [HttpGet("expired/batchQueue")]
        public async Task<IActionResult> GetExpiredSessionsBatchQueue()
        {
            var expiredSessions = await _dbService.GetSessionBatchQueuing();
            return Ok(expiredSessions);
        }

        // DELETE api/devicesession/expired
        [HttpDelete("deleteExpired")]
        public async Task<IActionResult> DeleteExpiredSessions()
        {
            await _dbService.DeleteExpiredSessionsAsync();
            return Ok("Deleted"); 
        }

        // GET api/devicesession/search
        [HttpGet("session")]
        public async Task<IActionResult> SearchBySessionText([FromQuery] string session)
        {
            var sessions = await _dbService.SearchBySessionTextAsync(session);
            return Ok(sessions); 
        }

     

        // GET API by test
        [HttpGet("purl")]
        public IActionResult GetByPurl([FromQuery] string purl)
        {
            var sessions =  _dbService.GetByPurl(purl);
            return Ok(sessions);
        }
    }
}
