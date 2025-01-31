const { promisify } = require("util");
const { createClient } = require("redis");
const { compress, uncompress } = require("./snappy");

const EXPIRY = 7200; // i.e. seconds = 2 hours

/**
 * RedisManager Singleton for handling Redis operations with compression.
 * @param {string} host
 * @param {number} port
 * @param {string} serviceName
 * @param {string} password
 * @param {object} options
 */
function RedisManager(host, port, serviceName, password, options) {
  if (!serviceName) {
    console.error("Service name is required");
    return;
  }

  const client = createClient({
    port,
    host,
    password,
    prefix: `${serviceName}:`,
    return_buffers: true,
    options,
  });
  const globalClient = createClient({
    port,
    host,
    password,
    return_buffers: true,
    options,
  });

  client.on("error", (err) => console.error("Redis Error:", err));
  client.on("connect", () => console.log("Redis connected successfully"));

  globalClient.on("error", (err) => console.error("Global Redis Error:", err));
  globalClient.on("connect", () =>
    console.log("Global redis connected successfully")
  );

  // Redis Promisified Methods
  const getAsync = promisify(client.get).bind(client);
  const setAsync = promisify(client.set).bind(client);
  const delAsync = promisify(client.del).bind(client);
  const keysAsync = promisify(client.keys).bind(client);

  const getAsyncGlobal = promisify(globalClient.get).bind(globalClient);
  const setAsyncGlobal = promisify(globalClient.set).bind(globalClient);

  // Get the value from Redis
  const getKey = async (key) => {
    const data = await getAsync(key);
    return data ? uncompress(data) : null;
  };

  // Set the value with compression
  const setKey = async (key, data, expiry = EXPIRY) => {
    const compressedData = compress(data);
    return setAsync(key, compressedData, "EX", expiry);
  };

  // Get the global key value
  const getGlobalKey = async (key) => {
    const data = await getAsyncGlobal(key);
    return data ? uncompress(data) : null;
  };

  // Set global key with compression
  const setGlobalKey = async (key, data, expiry = EXPIRY) => {
    const compressedData = compress(data);
    return setAsyncGlobal(key, compressedData, "EX", expiry);
  };

  // Delete key from Redis
  const removeKey = async (key) => {
    return delAsync(key);
  };

  // Get keys by regex pattern
  const getKeysByRegex = async (pattern) => {
    return keysAsync(pattern);
  };

  return {
    getKey,
    setKey,
    getGlobalKey,
    setGlobalKey,
    removeKey,
    getKeysByRegex,
    DEFAULT_EXPIRY: EXPIRY,
  };
}

module.exports = RedisManager;
