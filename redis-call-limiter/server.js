const express = require("express");
const Redis = require("ioredis");
const cors = require("cors"); 

const app = express();
const redis = new Redis(); // Connects to Redis at localhost:6379

app.use(cors());
app.use(express.json());

const MAX_ACTIVE_CALLS = 10;

// Start a call
app.post("/call", async (req, res) => {
  try {
    // Use Redis MULTI to ensure atomic increment
    const result = await redis.multi()
      .get("active_calls")
      .exec();

    let activeCalls = parseInt(result[0][1]) || 0;

    if (activeCalls >= MAX_ACTIVE_CALLS) {
      return res.status(429).json({
        error: "Max limit reached",
        active_calls: activeCalls,
        max_allowed: MAX_ACTIVE_CALLS
      });
    }

    // Atomically increment counters
    await redis.multi()
      .incr("total_calls_made")
      .incr("active_calls")
      .exec();

    res.json({ message: "Call started successfully" });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Resolve a call
app.post("/resolve", async (req, res) => {
  try {
    const activeCalls = parseInt(await redis.get("active_calls")) || 0;

    if (activeCalls > 0) {
      await redis.multi()
        .incr("total_calls_resolved")
        .decr("active_calls")
        .exec();
    }

    res.json({ message: "Call resolved successfully" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Stats
app.get("/stats", async (req, res) => {
  try {
    const totalCallsMade = parseInt(await redis.get("total_calls_made")) || 0;
    const totalCallsResolved = parseInt(await redis.get("total_calls_resolved")) || 0;
    const activeCalls = parseInt(await redis.get("active_calls")) || 0;

    res.json({
      total_calls_made: totalCallsMade,
      total_calls_resolved: totalCallsResolved,
      active_calls: activeCalls
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Internal server error" });
  }
});

app.listen(3000, () => console.log("Server running on port 3000"));
