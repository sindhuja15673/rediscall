// server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const bodyParser = require('body-parser');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const redis = new Redis();

app.use(cors());
app.use(bodyParser.json({ limit: '30mb' }));

// ---------- CONFIG ----------
const GLOBAL_RATE_PER_MIN = 10;
const MAX_ACTIVE_CALLS = 100;

const ACTIVE_SESSIONS = 'active_sessions';
const PENDING_QUEUE = 'pending_queue';
const PROCESSING_LIST = 'processing_list';
const COMPLETED_LIST = 'completed_list';
const ACTIVE_CALLS = 'active_calls';
const TOTAL_MADE = 'total_calls_made';
const TOTAL_RESOLVED = 'total_calls_resolved';

// ---------- HELPERS ----------
// function minuteKeySuffixUTC() {
//   const d = new Date();
//   return (
//     d.getUTCFullYear() +
//     String(d.getUTCMonth() + 1).padStart(2, '0') +
//     String(d.getUTCDate()).padStart(2, '0') +
//     String(d.getUTCHours()).padStart(2, '0') +
//     String(d.getUTCMinutes()).padStart(2, '0')
//   );
// }
function minuteKeySuffixUTC() {
  const d = new Date();
  const secondsBucket = d.getUTCSeconds() < 30 ? '00' : '30';

  return (
    d.getUTCFullYear() +
    String(d.getUTCMonth() + 1).padStart(2, '0') +
    String(d.getUTCDate()).padStart(2, '0') +
    String(d.getUTCHours()).padStart(2, '0') +
    String(d.getUTCMinutes()).padStart(2, '0') +
    secondsBucket
  );
}

// ---------- LUA SCRIPT ----------
const atomicPopLua = `
local active_calls = tonumber(redis.call('GET', KEYS[1]) or '0')
if active_calls >= tonumber(ARGV[1]) then return nil end

local globalCount = tonumber(redis.call('GET', KEYS[2]) or '0')
local globalLimit = tonumber(ARGV[2])
if globalCount >= globalLimit then return nil end

local jobId = redis.call('LPOP', KEYS[3])
if not jobId then return nil end

local jobKey = 'job:' .. jobId
local sessionId = redis.call('HGET', jobKey, 'sessionId')
local sessionRateKey = 'rate_count:session:' .. sessionId .. ':' .. ARGV[3]

local sessionCount = tonumber(redis.call('GET', sessionRateKey) or '0')
local activeSessions = tonumber(redis.call('SCARD', 'active_sessions'))
if activeSessions < 1 then activeSessions = 1 end

local sessionLimit = math.floor(globalLimit / activeSessions)
if sessionLimit < 1 then sessionLimit = 1 end

if sessionCount >= sessionLimit then
  redis.call('RPUSH', KEYS[3], jobId)
  return nil
end

redis.call('INCR', KEYS[1])
redis.call('INCR', KEYS[4])
redis.call('INCR', KEYS[2])
redis.call('INCR', sessionRateKey)

redis.call('EXPIRE', sessionRateKey, 120)
redis.call('EXPIRE', KEYS[2], 120)

redis.call('RPUSH', KEYS[5], jobId)
return jobId
`;

// ---------- UPLOAD ----------
app.post('/upload', async (req, res) => {
  const { sessionId, records } = req.body;
  if (!sessionId || !Array.isArray(records)) {
    return res.status(400).json({ error: 'sessionId and records[] required' });
  }

  const pipe = redis.pipeline();
  for (const record of records) {
    const id = uuidv4();
    pipe.hmset(`job:${id}`, {
      id,
      sessionId,
      payload: JSON.stringify(record),
      createdAt: Date.now()
    });
    pipe.rpush(PENDING_QUEUE, id);
  }

  await pipe.exec();
  await redis.sadd(ACTIVE_SESSIONS, sessionId);

  await emitStats();
  await triggerProcessingOnce();

  res.json({ message: 'Uploaded', count: records.length });
});

// ---------- STATS ----------
app.get('/stats', async (req, res) => {
  const minute = minuteKeySuffixUTC();

  const pending = await redis.llen(PENDING_QUEUE);
  const processing = await redis.llen(PROCESSING_LIST);
  const completed = await redis.llen(COMPLETED_LIST);
  const active = Number(await redis.get(ACTIVE_CALLS)) || 0;
  const totalMade = Number(await redis.get(TOTAL_MADE)) || 0;
  const totalResolved = Number(await redis.get(TOTAL_RESOLVED)) || 0;
  const globalRate = Number(await redis.get(`rate_count:global:${minute}`)) || 0;

  const sessions = await redis.smembers(ACTIVE_SESSIONS);
  const sessionRates = {};
  for (const s of sessions) {
    sessionRates[s] =
      Number(await redis.get(`rate_count:session:${s}:${minute}`)) || 0;
  }

  res.json({
    pending,
    processing,
    completed,
    active,
    totalMade,
    totalResolved,
    globalRatePerMin: GLOBAL_RATE_PER_MIN,
    globalRateThisMin: globalRate,
    sessionRates
  });
});

// ---------- RESET ----------
app.post('/reset', async (req, res) => {
  const keys = [
    ACTIVE_CALLS,
    PENDING_QUEUE,
    PROCESSING_LIST,
    COMPLETED_LIST,
    TOTAL_MADE,
    TOTAL_RESOLVED,
    ACTIVE_SESSIONS
  ];
  await redis.del(...keys);

  const rateKeys = await redis.keys('rate_count:*');
  if (rateKeys.length) await redis.del(...rateKeys);

  const jobKeys = await redis.keys('job:*');
  if (jobKeys.length) await redis.del(...jobKeys);

  await emitStats();
  res.json({ message: 'Reset completed' });
});

// ---------- SOCKET ----------
io.on('connection', socket => {
  socket.on('request-stats', async () => {
    socket.emit('stats', await getStats());
  });
});

async function emitStats() {
  io.emit('stats', await getStats());
}

async function getStats() {
  const minute = minuteKeySuffixUTC();
  const sessions = await redis.smembers(ACTIVE_SESSIONS);
  const sessionRates = {};

  for (const s of sessions) {
    sessionRates[s] =
      Number(await redis.get(`rate_count:session:${s}:${minute}`)) || 0;
  }

  return {
    pending: await redis.llen(PENDING_QUEUE),
    processing: await redis.llen(PROCESSING_LIST),
    completed: await redis.llen(COMPLETED_LIST),
    active: Number(await redis.get(ACTIVE_CALLS)) || 0,
    totalMade: Number(await redis.get(TOTAL_MADE)) || 0,
    totalResolved: Number(await redis.get(TOTAL_RESOLVED)) || 0,
    globalRatePerMin: GLOBAL_RATE_PER_MIN,
    globalRateThisMin:
      Number(await redis.get(`rate_count:global:${minute}`)) || 0,
    sessionRates
  };
}

// ---------- PROCESSING ----------
async function tryProcessOnce() {
  const minute = minuteKeySuffixUTC();
  const keys = [
    ACTIVE_CALLS,
    `rate_count:global:${minute}`,
    PENDING_QUEUE,
    TOTAL_MADE,
    PROCESSING_LIST
  ];
  const argv = [
    String(MAX_ACTIVE_CALLS),
    String(GLOBAL_RATE_PER_MIN),
    minute
  ];

  const jobId = await redis.eval(atomicPopLua, keys.length, ...keys, ...argv);
  if (!jobId) return null;

  const job = await redis.hgetall(`job:${jobId}`);
  job.payload = JSON.parse(job.payload);
  return job;
}

async function handleJob(job) {
  io.emit('job_started', { jobId: job.id, sessionId: job.sessionId });

  await new Promise(r => setTimeout(r, 1500 + Math.random() * 3000));

  const pipe = redis.multi();
  pipe.lrem(PROCESSING_LIST, 1, job.id);
  pipe.decr(ACTIVE_CALLS);
  pipe.incr(TOTAL_RESOLVED);
  pipe.rpush(COMPLETED_LIST, JSON.stringify(job));
  await pipe.exec();

  const pending = await redis.lrange(PENDING_QUEUE, 0, -1);
  const processing = await redis.lrange(PROCESSING_LIST, 0, -1);

  let stillActive = false;
  for (const id of [...pending, ...processing]) {
    if ((await redis.hget(`job:${id}`, 'sessionId')) === job.sessionId) {
      stillActive = true;
      break;
    }
  }

  if (!stillActive) {
    await redis.srem(ACTIVE_SESSIONS, job.sessionId);
  }

  io.emit('job_completed', job);
  await emitStats();
}

async function triggerProcessingOnce() {
  while (true) {
    const job = await tryProcessOnce();
    if (!job) break;
    handleJob(job);
  }
}

setInterval(triggerProcessingOnce, 1000);

// ---------- INIT ----------
(async () => {
  await redis.setnx(ACTIVE_CALLS, 0);
  await redis.setnx(TOTAL_MADE, 0);
  await redis.setnx(TOTAL_RESOLVED, 0);
})();

const PORT = 3000;
server.listen(PORT, () => console.log(`Server running on ${PORT}`));
