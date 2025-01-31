// Modules
const Queue = require("bull");

const serviceTypes = {
  SQS: "sqs",
  BULL: "bull",
  KAFKA: "kafka",
  TOPIC: "topic",
};

class QueueService {
  #bull = null;
  #serviceType = serviceTypes.BULL;
  #serviceConfig = null;
  #messageHandler = null;
  #onConsumerError = null;
  #maxInProgress = 1;
  #isBatchPull = false;

  /**
   * Initialize new instance of QueueService
   * @param {QueueServiceConfig} queueServiceConfig: { serviceType, queueUrl, queueName, queuePrefix, redisConfig, projectId, topicName, subscriptionName, accessKeyId, secretAccessKey, clientId, brokers, kafkaTopicName, groupId, logLevel }
   */
  constructor(queueServiceConfig) {
    this.#serviceType = queueServiceConfig.serviceType;
    this.#serviceConfig = {
      // for BULL
      queueName: queueServiceConfig.queueName,
      queuePrefix: queueServiceConfig.queuePrefix,
      redisConfig: queueServiceConfig.redisConfig,
    };
    this.#init();
  }

  #init = () => {
    const serviceType = this.#serviceType;
    if (serviceType === serviceTypes.BULL) {
      const { queueName, queuePrefix, redisConfig } = this.#serviceConfig;
      if (!queueName) throw new Error(`Missing queueName`);
      if (!queuePrefix) throw new Error(`Missing queuePrefix`);
      if (!redisConfig) throw new Error(`Missing redisConfig`);
      this.#bull = new Queue(queueName, {
        prefix: queuePrefix,
        redis: redisConfig,
        defaultJobOptions: { removeOnComplete: true },
      });
      return;
    }
    throw new Error("Queue service did not recognise");
  };

  /**
   * Push payload, additionally with a delay to either bull or sqs
   * (only for bull):: payload :: either can be data that directly need to pass to queue or
   *      it can be array of args since bull.add function is overloaded function which supports different type of args,
   *      ex. payload = {key: value, key2: value2} or any other data type
   *      ex. payload = ['job-path-name', { method: 'POST', content: anyDataType, etc.. }] (using in vartalap)
   * @param {Object} payload data that need to pass to queue | data[] if isBatch : true.
   * @param {number} delay message delay in ms
   * @param {boolean} spreadPayload flag for spreading payload if array (only for bull)
   * @param {Object} options bull queue options like attempt, backoff etc (only for bull).
   * @param {Boolean} isBatch topic queue options to publish in batches
   * @param {Object} batchOptions topic queue batchOption { maxMessages, maxWaitTime }.
   * @returns
   */
  publishMessage = (
    payload,
    delay = 0,
    spreadPayload = false,
    options = {}
  ) => {
    const serviceType = this.#serviceType;
    if (serviceType === serviceTypes.BULL) {
      return this.#publishToBull(payload, delay, spreadPayload, options);
    }
  };

  /**
   * Pushes payload to bull queue, additionally with delay
   * @param {Object} payload
   * @param {number} delay
   * @returns
   */
  #publishToBull = (
    payload,
    delay = 0,
    destructurePayload = false,
    options = {}
  ) => {
    const bull = this.#bull;
    return new Promise((resolve, reject) => {
      let res;
      if (destructurePayload) {
        res = bull.add(...payload.filter(Boolean), {
          ...options,
          removeOnComplete: true,
          delay,
        });
      } else {
        res = bull.add(payload, { ...options, removeOnComplete: true, delay });
      }
      resolve(res);
    });
  };

  /**
   * Publish to Bulll in Array formatted data
   */
  publishToBullInBatches = async (
    formattedDataArr,
    delay = 15000,
    batchSize = 20
  ) => {
    const bullQueue = this.#bull;
    if (!formattedDataArr.length) {
      return;
    }
    for (const formattedData of formattedDataArr) {
      let allJobs = await bullQueue.getJobs(); // All Jobs Present in the Queue
      if (allJobs.length) {
        allJobs = allJobs.filter(Boolean);
        let isAllBatchFilled = true;
        allJobs.forEach((job) => {
          if (job && job.data && job.data?.length !== batchSize) {
            isAllBatchFilled = false;
            return;
          }
        });
        if (isAllBatchFilled) {
          // Add New Batch in the Bull
          await bullQueue.add([formattedData], {
            removeOnComplete: true,
            delay: delay,
          }); // Array of 1 items initially , expired in 15 seconds
        } else {
          // Add Data to any non-filled existing Batch and exit , affect delay with 0 if batch is complete
          const descSortedJobData = allJobs.sort(
            (a, b) => b.data.length - a.data.length
          ); // Job Data Sorted By their Batches Length , this is required . Fill the Batch first who is nearest to the complete.

          for (const jobData of descSortedJobData) {
            if (jobData.data.length < batchSize) {
              const storedJobData = jobData.data;
              const updatedJobData = [...storedJobData, formattedData];
              await jobData.update(updatedJobData);
              // Active Job Process
              try {
                updatedJobData.length === batchSize &&
                  (await jobData.promote()); // Batch is filled , make it in active state
              } catch (err) {
                // if error comes on promoting , it means it is already in active state , proceed to next data process , break the inner loop
                break;
              }
              // proceed to next data process , break the inner loop
              break;
            }
          }
        }
      } else {
        // Simpy Push , No Data
        await bullQueue.add([formattedData], {
          removeOnComplete: true,
          delay: delay,
        }); // Array of 1 items initially , expired in 30 seconds
      }
    }
  };

  addListener = ({
    onConsumerError,
    messageHandler,
    maxInProgress = 1,
    isBatchPull = false,
  }) => {
    this.#onConsumerError = onConsumerError;
    this.#messageHandler = messageHandler;
    this.#maxInProgress = maxInProgress;
    this.#isBatchPull = isBatchPull;
  };

  startListener = () => {
    const serviceType = this.#serviceType;
    if (serviceType === serviceTypes.BULL) {
      return this.#addListenerToBull();
    }
  };

  /**
   * Creates a consumer and listens for bull
   */
  #addListenerToBull = () => {
    const bull = this.#bull;

    const onConsumerError = this.#onConsumerError;
    const messageHandler = this.#messageHandler;

    bull.process(async (job) => await messageHandler(job));

    bull.on("error", onConsumerError);
  };

  parseQueueMsg = (message) => {
    const serviceType = this.#serviceType;

    if (serviceType === serviceTypes.BULL) {
      return message;
    }
  };
}
module.exports = {
  QueueService,
  serviceTypes,
};
