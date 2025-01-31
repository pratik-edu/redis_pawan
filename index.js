const RedisManager = require('./redis').default;
const { QueueService, serviceTypes } = require('./queue');

export default {
  RedisManager,
  QueueService,
  serviceTypes
};