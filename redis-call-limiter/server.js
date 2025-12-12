
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
const redis = new Redis(); // default localhost:6379

app.use(cors());
app.use(bodyParser.json({ limit: '30mb' }));

// ---------- CONFIG ----------
const PER_SESSION_RATE = 5;        // per-session calls allowed per minute
const GLOBAL_RATE_PER_MIN = 10;    // global allowed calls per minute
const MAX_ACTIVE_CALLS = 100;      // cap of concurrent active calls

// Redis key names
const PENDING_QUEUE = 'pending_queue';
const PROCESSING_LIST = 'processing_list';
const COMPLETED_LIST = 'completed_list';
const ACTIVE_CALLS = 'active_calls';
const TOTAL_MADE = 'total_calls_made';
const TOTAL_RESOLVED = 'total_calls_resolved';

// Helper to get minute-suffix (UTC) used in rate keys
function minuteKeySuffixUTC(){
  const d = new Date();
  const YYYY = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, '0');
  const DD = String(d.getUTCDate()).padStart(2, '0');
  const HH = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  return `${YYYY}${MM}${DD}${HH}${mm}`;
}

// Atomic Lua script to pop one job if limits allow
const atomicPopLua = `
local active_calls = tonumber(redis.call('GET', KEYS[1]) or '0')
local max_active = tonumber(ARGV[1])
if active_calls >= max_active then return nil end

local globalKey = KEYS[2]
local globalCount = tonumber(redis.call('GET', globalKey) or '0')
local globalLimit = tonumber(ARGV[2])
if globalCount >= globalLimit then return nil end

local jobId = redis.call('LPOP', KEYS[3])
if not jobId then return nil end

local jobKey = 'job:' .. jobId
local sessionId = redis.call('HGET', jobKey, 'sessionId') or ''
local sessionRateKey = 'rate_count:session:' .. sessionId .. ':' .. ARGV[4]
local sessionCount = tonumber(redis.call('GET', sessionRateKey) or '0')
local sessionLimit = tonumber(ARGV[3])
if sessionCount >= sessionLimit then
  -- put job back
  redis.call('RPUSH', KEYS[3], jobId)
  return nil
end

-- allowed: mark started
redis.call('INCR', KEYS[1])        -- active_calls
redis.call('INCR', KEYS[4])        -- total_made
redis.call('INCR', globalKey)      -- global minute counter
redis.call('INCR', sessionRateKey) -- session minute counter
redis.call('EXPIRE', sessionRateKey, 150)
redis.call('EXPIRE', globalKey, 150)

redis.call('RPUSH', KEYS[5], jobId)
return jobId
`;

// ---------- API: upload ----------
app.post('/upload', async (req, res) => {
  try{
    const { sessionId, records } = req.body;
    if (!sessionId || !Array.isArray(records)) return res.status(400).json({ error: 'sessionId and records[] required' });

    const pipe = redis.pipeline();
    for (let i=0;i<records.length;i++){
      const id = uuidv4();
      const jobKey = `job:${id}`;
      const jobObj = { id, sessionId, payload: JSON.stringify(records[i]), createdAt: Date.now() };
      pipe.hmset(jobKey, jobObj);
      pipe.rpush(PENDING_QUEUE, id);
    }
    await pipe.exec();

    // emit stats and try processing immediately
    await emitStats();
    await triggerProcessingOnce();

    return res.json({ message: 'uploaded', count: records.length });
  }catch(err){
    console.error('upload err', err); res.status(500).json({ error: 'internal' });
  }
});

// ---------- API: resolve (manual) ----------
app.post('/resolve', async (req, res) => {
  try{
    const { jobId, success = true } = req.body;
    if (!jobId) return res.status(400).json({ error: 'jobId required' });

    const jobHashKey = `job:${jobId}`;
    const exists = await redis.exists(jobHashKey);
    if (!exists) return res.status(404).json({ error: 'job not found' });

    const result = { jobId, success: success ? 'success' : 'failure', resolvedAt: Date.now() };
    const pipe = redis.multi();
    pipe.lrem(PROCESSING_LIST, 1, jobId);
    pipe.decr(ACTIVE_CALLS);
    pipe.incr(TOTAL_RESOLVED);
    pipe.rpush(COMPLETED_LIST, JSON.stringify(result));
    await pipe.exec();

    await emitStats();
    await triggerProcessingOnce();
    return res.json({ message: 'resolved' });
  }catch(err){ console.error('resolve err', err); res.status(500).json({ error: 'internal' }); }
});

// ---------- API: stats ----------
app.get('/stats', async (req, res) => {
  try{
    const pending = await redis.llen(PENDING_QUEUE);
    const processing = await redis.llen(PROCESSING_LIST);
    const completed = await redis.llen(COMPLETED_LIST);
    const active = parseInt(await redis.get(ACTIVE_CALLS)) || 0;
    const totalMade = parseInt(await redis.get(TOTAL_MADE)) || 0;
    const totalResolved = parseInt(await redis.get(TOTAL_RESOLVED)) || 0;
    const globalRateKey = `rate_count:global:${minuteKeySuffixUTC()}`;
    const globalRate = parseInt(await redis.get(globalRateKey)) || 0;
    const rateA = parseInt(await redis.get(`rate_count:session:A:${minuteKeySuffixUTC()}`)) || 0;
    const rateB = parseInt(await redis.get(`rate_count:session:B:${minuteKeySuffixUTC()}`)) || 0;

    res.json({ pending, processing, completed, active, totalMade, totalResolved, globalRatePerMin: GLOBAL_RATE_PER_MIN, globalRateThisMin: globalRate, perSessionLimit: PER_SESSION_RATE, sessionRates: { A: rateA, B: rateB } });
  }catch(err){ console.error('stats err', err); res.status(500).json({ error: 'internal' }); }
});

// ---------- API: export completed ----------
app.get('/export-completed', async (req, res) => {
  try{
    const arr = await redis.lrange(COMPLETED_LIST, 0, -1);
    const parsed = arr.map(x => { try { return JSON.parse(x); } catch { return { raw: x }; } });
    res.json(parsed);
  }catch(err){ console.error(err); res.status(500).json({ error: 'internal' }); }
});

// ---------- API: reset ----------
app.post('/reset', async (req, res) => {
  try{
    const keysToDelete = [ACTIVE_CALLS, PENDING_QUEUE, PROCESSING_LIST, COMPLETED_LIST, TOTAL_MADE, TOTAL_RESOLVED];
    await redis.del(...keysToDelete);

    const rateKeys = await redis.keys('rate_count:*');
    if (rateKeys.length) await redis.del(...rateKeys);

    const jobKeys = await redis.keys('job:*');
    if (jobKeys.length) await redis.del(...jobKeys);

    await emitStats();
    return res.json({ message: 'Reset completed successfully' });
  }catch(err){ console.error('Reset error', err); res.status(500).json({ error: 'Failed to reset' }); }
});

// ---------- SOCKET.IO ----------
io.on('connection', socket => {
  console.log('socket connected', socket.id);
  socket.on('request-stats', async () => { const stats = await getStats(); socket.emit('stats', stats); });
});

async function emitStats(){ const stats = await getStats(); io.emit('stats', stats); }

async function getStats(){
  const pending = await redis.llen(PENDING_QUEUE);
  const processing = await redis.llen(PROCESSING_LIST);
  const completed = await redis.llen(COMPLETED_LIST);
  const active = parseInt(await redis.get(ACTIVE_CALLS)) || 0;
  const totalMade = parseInt(await redis.get(TOTAL_MADE)) || 0;
  const totalResolved = parseInt(await redis.get(TOTAL_RESOLVED)) || 0;
  const globalRate = parseInt(await redis.get(`rate_count:global:${minuteKeySuffixUTC()}`)) || 0;
  const rateA = parseInt(await redis.get(`rate_count:session:A:${minuteKeySuffixUTC()}`)) || 0;
  const rateB = parseInt(await redis.get(`rate_count:session:B:${minuteKeySuffixUTC()}`)) || 0;
  return { pending, processing, completed, active, totalMade, totalResolved, globalRatePerMin: GLOBAL_RATE_PER_MIN, globalRateThisMin: globalRate, perSessionLimit: PER_SESSION_RATE, sessionRates: { A: rateA, B: rateB } };
}

// ---------- PROCESSING: try to pop and start jobs ----------
async function tryProcessOnce(){
  try{
    const minuteSuffix = minuteKeySuffixUTC();
    const keys = [ACTIVE_CALLS, `rate_count:global:${minuteSuffix}`, PENDING_QUEUE, TOTAL_MADE, PROCESSING_LIST];
    const argv = [String(MAX_ACTIVE_CALLS), String(GLOBAL_RATE_PER_MIN), String(PER_SESSION_RATE), minuteSuffix];
    const res = await redis.eval(atomicPopLua, keys.length, ...keys, ...argv);
    if (!res) return null;
    const jobId = res;
    const jobKey = `job:${jobId}`;
    const jobHash = await redis.hgetall(jobKey);
    if (jobHash.payload){ try { jobHash.payload = JSON.parse(jobHash.payload); } catch(e){} }
    return jobHash;
  }catch(err){ console.error('tryProcessOnce err', err); return null; }
}

async function handleJob(job){
  try{
    io.emit('job_started', { jobId: job.id, sessionId: job.sessionId, startedAt: Date.now() });
    const durationMs = 1500 + Math.floor(Math.random() * 3000);
    await new Promise(r => setTimeout(r, durationMs));

    const result = { jobId: job.id, sessionId: job.sessionId, status: 'success', startedAt: Date.now() - durationMs, resolvedAt: Date.now() };
    const pipe = redis.multi();
    pipe.lrem(PROCESSING_LIST, 1, job.id);
    pipe.decr(ACTIVE_CALLS);
    pipe.incr(TOTAL_RESOLVED);
    pipe.rpush(COMPLETED_LIST, JSON.stringify(result));
    await pipe.exec();

    io.emit('job_completed', result);
    await emitStats();
    await triggerProcessingOnce();
  }catch(err){ console.error('handleJob err', err); }
}

async function triggerProcessingOnce(){
  let started = 0;
  while(true){
    const job = await tryProcessOnce();
    if (!job) break;
    started++;
    // do not await; run concurrently
    handleJob(job).catch(e => console.error('handleJob background err', e));
  }
  if (started > 0) await emitStats();
  return started;
}

// background loop
setInterval(async () => { try{ await triggerProcessingOnce(); }catch(e){ console.error('background err', e); } }, 1000);

// initialize counters
(async function init(){
  await redis.setnx(ACTIVE_CALLS, 0);
  await redis.setnx(TOTAL_MADE, 0);
  await redis.setnx(TOTAL_RESOLVED, 0);
})();

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server listening on ${PORT}`));
