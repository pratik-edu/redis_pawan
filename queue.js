const { Queue, Worker, QueueScheduler, Job } = require("bullmq");

const serviceTypes = {
  BULLMQ: "bullmq",
};

class QueueService {
  #queue = null;
  #worker = null;
  #scheduler = null;
  #serviceConfig = null;
  #messageHandler = null;
  #onConsumerError = null;
  #maxInProgress = 1;

  constructor(queueServiceConfig) {
    this.#serviceConfig = {
      queueName: queueServiceConfig.queueName,
      redisConfig: queueServiceConfig.redisConfig,
    };
    this.#init();
  }

  #init = () => {
    const { queueName, redisConfig } = this.#serviceConfig;
    if (!queueName || !redisConfig)
      throw new Error(`Missing queue configuration`);
    this.#queue = new Queue(queueName, { connection: redisConfig });
    this.#scheduler = new QueueScheduler(queueName, {
      connection: redisConfig,
    });

    // Check if Redis is connected successfully
    this.#queue.on("ready", () => {
      console.log(`Redis connected successfully for queue ${queueName}`);
    });

    this.#queue.on("error", (err) => {
      console.error(`Redis connection error for queue ${queueName}:`, err);
    });
  };

  publishMessage = async (payload, delay = 0, options = {}) => {
    return this.#queue.add("job", payload, {
      delay,
      removeOnComplete: true,
      ...options,
    });
  };

  publishToBullInBatches = async (
    formattedDataArr,
    delay = 15000,
    batchSize = 20
  ) => {
    if (!formattedDataArr.length) return;

    for (const formattedData of formattedDataArr) {
      const jobs = await this.#queue.getJobs();
      let existingBatch = jobs.find((job) => job.data.length < batchSize);

      if (existingBatch) {
        const updatedJobData = [...existingBatch.data, formattedData];
        await existingBatch.update(updatedJobData);
        if (updatedJobData.length === batchSize) await existingBatch.promote();
      } else {
        await this.#queue.add("batch-job", [formattedData], {
          removeOnComplete: true,
          delay,
        });
      }
    }
  };

  addListener = ({ onConsumerError, messageHandler, maxInProgress = 1 }) => {
    this.#onConsumerError = onConsumerError;
    this.#messageHandler = messageHandler;
    this.#maxInProgress = maxInProgress;
  };

  startListener = () => {
    const { queueName, redisConfig } = this.#serviceConfig;
    this.#worker = new Worker(
      queueName,
      async (job) => {
        if (this.#messageHandler) {
          await this.#messageHandler(job);
        }
      },
      { connection: redisConfig, concurrency: this.#maxInProgress }
    );
    this.#worker.on("error", this.#onConsumerError);
  };

  parseQueueMsg = (message) => {
    return message;
  };
}

module.exports = QueueService;
