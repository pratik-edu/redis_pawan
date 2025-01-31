/**
 * Create an singleton object of the redis and set the object and get the data w.r.t. key .
 * Also it do compression of data before insertion in the redis.
 * Using node-redis for redis connection and snappy for the data compression and uncompress.
 * For now it supports only getKey, setKey, removeKey , in future can implement other required DS.
 */

import { promisify } from "util";
import { createClient } from "redis";
const EXPIRY = 7200; // i.e. seconds = 2 hour

import { compress, uncompress } from "./snappy";
/**
 * Return methods i.e. getKey, setKey
 * @param {string} host
 * @param {number} port
 * @param {string} serviceName
 * @param {object} options
 */
function redis_pawan(host, port, serviceName, password, options) {
  if (!serviceName || serviceName === "") {
    console.error("Service name is required");
    return "Service name is required";
  }

  const client = createClient({
    port: port,
    host: host,
    password: password,
    prefix: `${serviceName}:`,
    return_buffers: true,
    options: options,
  });

  const globalClient = createClient({
    port: port,
    host: host,
    password: password,
    return_buffers: true,
    options: options,
  });

  client.on("error", (err) => {
    console.error("Redis Error " + err);
  });

  client.on("connect", () => {
    console.log("redis connected successfully");
  });

  globalClient.on("error", (err) => {
    console.error("Global Redis Error " + err);
  });

  globalClient.on("connect", () => {
    console.log("global redis connected successfully");
  });

  const getAsync = promisify(client.get).bind(client);
  const setAsync = promisify(client.set).bind(client);
  const setNxAsync = promisify(client.setnx).bind(client);
  const setTtlAsync = promisify(client.expire).bind(client);
  const delAsync = promisify(client.del).bind(client);
  const keysAsync = promisify(client.keys).bind(client);
  const addKeyToSetAsync = promisify(client.sadd).bind(client);
  const removeKeysFromSetAsync = promisify(client.srem).bind(client);
  const getSetMembersAsync = promisify(client.smembers).bind(client);
  const incrByAsync = promisify(client.incrby).bind(client);
  const mgetAsync = promisify(client.mget).bind(client);
  const msetAsync = promisify(client.mset).bind(client);

  const redisMulti = client.multi();
  const execAsync = promisify(redisMulti.exec).bind(redisMulti);

  const getAsyncGlobal = promisify(globalClient.get).bind(globalClient);
  const setAsyncGlobal = promisify(globalClient.set).bind(globalClient);
  const evalAsyncGlobal = promisify(globalClient.eval).bind(globalClient);
  /**
   * Return the data with respect to the key.
   * @param {String} key
   */
  const getKey = async (key) => {
    const data = await getAsync(key);
    return data ? uncompress(data) : "";
  };

  /**
   * Get the value for a key that was not compressed
   * @param {String} key
   */
  const getUncompressedKey = (key) => {
    return getAsync(key);
  };

  /**
   * Returns all keys which match the RegEx pattern.
   * @param {String} pattern
   */
  const getKeysByRegex = async (pattern) => {
    return keysAsync(pattern);
  };

  /**
   * Set the value with respect to key
   * @param {String} key
   * @param {string} data
   */
  const setKey = async (key, data, expiry) => {
    const compressedData = compress(data);
    const setSucc = await setAsync(key, compressedData, "EX", expiry || EXPIRY);
    return setSucc;
  };

  /**
   * Set the value with respect to key
   * @param {String} key
   * @param {string} data
   */
  const setGlobalKey = async (key, data, expiry) => {
    const compressedData = compress(data);
    const setSucc = await setAsyncGlobal(
      key,
      compressedData,
      "EX",
      expiry || EXPIRY
    );
    return setSucc;
  };

  /**
   * Set the value of key if it does not exist
   * @param {String} key
   * @param {String} data
   */
  const setKeyIfNotExists = async (key, data) => {
    return setNxAsync(key, data);
  };

  /**
   * Set the key expiry in seconds
   * @param {String} key
   * @param {Number} ttl
   */
  const setKeyTTL = async (key, ttl) => {
    return setTtlAsync(key, ttl);
  };

  /**
   * Bulk Set the keys expiry in seconds
   * @param {array} key
   * @param {Number} ttl
   */
  const setKeyTTLs = async (keys = [], ttl = EXPIRY) => {
    //set ttl to keys.
    keys.forEach(async (element) => {
      redisMulti.expire(element, ttl);
    });
    return await execAsync();
  };

  /**
   * Delete the value with respect to key
   * @param {String} key
   */
  const removeKey = async (key) => {
    const delSucc = await delAsync(key);
    return delSucc;
  };

  /**
   * Adds list of members to set
   * @param {String} setName
   * @param {Array} elements
   */
  const addElemToSet = (setName, elements) => {
    return addKeyToSetAsync(setName, elements);
  };

  /**
   * Removes list of members from set
   * @param {String} setName
   * @param {Array} elements
   */
  const removeElemFromSet = (setName, elements) => {
    return removeKeysFromSetAsync(setName, elements);
  };

  /**
   * Returns list of members present in set
   * @param {String} setName
   */
  const getElemsInSet = (setName) => {
    return getSetMembersAsync(setName);
  };

  /**
   * Return the data if exists in cache, use this function as a middleware
   * req is useful when need to create a string on the basis of URL params
   * ToDo: Need to think more on the creation of keys
   * @param {request body} req
   * @param {string} key
   */
  const isCached = async (req, res, next) => {
    const data = await getKey(req.__cache_key);
    if (data) {
      return res.sendformat(JSON.parse(data));
    }
    next();
  };

  const defaultKey = (req) => (req ? req.url : "");

  /**
   * Set the __cache_key in the request by using custom logic for each route.
   * @param {*} parseKey it can be function or string.
   */
  const generateKey = (parseKey = defaultKey) => {
    return (req, res, next) => {
      req.__cache_key =
        typeof parseKey === "function" ? parseKey(req) : parseKey;
      next();
    };
  };

  /**
   * Increments a key by incrValue. If key does not exist, initializes key to 0 then updates.
   * @param {String} key
   * @param {Number} incrValue
   */
  const incrementKeyByValue = (key, incrValue) => {
    return incrByAsync(key, incrValue);
  };

  /**
   *
   * @param {array} keys
   * @returns array list of values at the specified keys.
   */
  const getValues = async (keys = []) => {
    if (!keys || !keys.length) return [];
    const compressedData = await mgetAsync(keys);
    const uncompressedData = compressedData.map((data) => {
      if (data) {
        return uncompress(data);
      }
      return null;
    });
    return uncompressedData;
  };

  /**
   *
   * @param {array} keys
   * @param {number} batchSize
   */
  const getValuesInBatches = async (keys = [], batchSize = 100) => {
    if (keys.length === 0) return [];

    const batchPromises = [];
    for (let i = 0; i < keys.length; i += batchSize) {
      const batchKeys = keys.slice(i, i + batchSize);
      batchPromises.push(mgetAsync(batchKeys));
    }

    const batchResults = await Promise.all(batchPromises);
    // Flatten results and uncompress data
    const uncompressedResults = [];
    batchResults.forEach((batch) => {
      batch.forEach((compressedData) => {
        uncompressedResults.push(
          compressedData ? uncompress(compressedData) : null
        );
      });
    });

    return uncompressedResults;
  };

  /**
   * set multiple key and values at same time and also add expiry for each key.
   * @param {array} keys
   * @param {array} values
   * @param {number} ttl
   * @returns object
   */
  const setValues = async (keys = [], values = [], ttl = EXPIRY) => {
    const combinedData = [];

    for (var i = 0; i < keys.length; i++) {
      const compressedData = compress(values[i]);
      combinedData.push(keys[i]);
      combinedData.push(compressedData);
    }
    const response = await msetAsync(combinedData);

    //set ttl to keys.
    await setKeyTTLs(keys, ttl);

    return response;
  };

  /**
   * caches data returned by fn for ttl seconds.
   * @param fn - function whose response is to be cached
   * @param keyGenerator - creates key for caching data from passed arguments. can be async. if keyGenerator does not return a key, fresh data is returned
   * @param ttl - in seconds
   * @param redisKeyPrefix
   * @return {function(...[*]): Promise<null|*>}
   */
  const cacheFn = (
    fn,
    keyGenerator,
    ttl = EXPIRY,
    { redisKeyPrefix = "cacheFn:", isGlobal = false } = {}
  ) => {
    const getKeyFn = isGlobal ? getGlobalKey : getKey;
    const setKeyFn = isGlobal ? setGlobalKey : setKey;
    if (!(fn && keyGenerator && ttl))
      throw new Error(`need all 3 arguments fn, keyGenerator and ttl.`);
    return async (...args) => {
      const subKey = await keyGenerator(...args);
      const key = subKey ? `${redisKeyPrefix}${subKey}` : null;
      const stringifiedRedisData = key ? await getKeyFn(key) : null;
      const redisData = stringifiedRedisData
        ? JSON.parse(stringifiedRedisData)
        : null;
      if (redisData) return redisData;
      else {
        const data = await fn(...args);
        if (key) await setKeyFn(key, JSON.stringify(data), ttl);
        return data;
      }
    };
  };

  /**
   * Return the data with respect to the global key.
   * @param {String} key
   */
  const getGlobalKey = async (key) => {
    const data = await getAsyncGlobal(key);
    return data ? uncompress(data) : "";
  };

  // function to delete keys matching a pattern using Lua script
  const removeKeysByPattern = async (pattern) => {
    if (pattern.length < 5) {
      throw new Error(`Pattern length must be greater than 5`);
    } else if (Array.from(pattern).every((char) => char === "*")) {
      throw new Error(`Pattern can't have all *`);
    }
    return await evalAsyncGlobal(
      `for _,key in ipairs(redis.call('KEYS', ARGV[1])) do redis.call('DEL', key) end`,
      0,
      pattern
    );
  };

  return {
    getKey,
    getKeysByRegex,
    setKey,
    getUncompressedKey,
    setKeyIfNotExists,
    setKeyTTL,
    removeKey,
    addElemToSet,
    removeElemFromSet,
    getElemsInSet,
    generateKey,
    isCached,
    incrementKeyByValue,
    cacheFn,
    DEFAULT_EXPIRY: EXPIRY,
    getValues,
    getValuesInBatches,
    setValues,
    setKeyTTLs,
    getGlobalKey,
    removeKeysByPattern,
  };
}

export default redis_pawan;
