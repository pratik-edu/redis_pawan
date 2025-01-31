/**
 * Use to do compress data before inserting in the redis.
 * uncompress the data before sending the service back
 */

const snappy = require("snappy");

/**
 * Return the compress version of given object.
 * @param {object} data
 */
const compress = data => {
  try {
    const compressed = snappy.compressSync(data);
    return compressed;
  } catch(err) {
    console.error("Error in compression ", err);
    return err;
  }
};

/**
 * Return the uncompressed version of the given object
 * @param {object} compressed
 */
const uncompress = compressed => {
  try {
    return snappy.uncompressSync(compressed, { asBuffer: false }); 
  } catch(err) {
    console.log("Error during decompression ", err);
    return err;
  }
};

module.exports = { compress, uncompress };
