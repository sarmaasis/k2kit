export type { BackoffRange } from "./backoff.js";
export {
  estimateRecordBytes,
  K2Client,
  type K2ClientOptions,
  K2TimeoutError,
  MAX_PRODUCE_REQUEST_BYTES,
  MAX_RECORD_BYTES,
  type ProduceBatchedOptions,
  type ProduceProgress,
} from "./client.js";
export {
  type BatchEvent,
  Consumer,
  type ConsumerEvents,
  type ConsumerStats,
  type RunOptions,
} from "./consumer.js";
export {
  DEAD_LETTER_HEADERS,
  type DeadLetter,
  type DeadLetterContext,
  type DeadLetterOptions,
  type DeadLetterSend,
  deadLetterToStream,
} from "./dead-letter.js";
export { MemoryDedupeStore } from "./dedupe.js";
export { isK2Error, K2Error, RETRYABLE_CONSUME_CODES } from "./errors.js";
export {
  type ConsumerOptions,
  consoleLogger,
  type ErrorContext,
  type ErrorPhase,
  type RetryBackoff,
} from "./options.js";
export { ID_HEADER } from "./record.js";
export {
  type RedisDedupeClient,
  RedisDedupeStore,
  type RedisDedupeStoreOptions,
} from "./redis-dedupe.js";
export type {
  Batch,
  DedupeStore,
  Handler,
  HandlerContext,
  K2Record,
  Logger,
  ProduceOptions,
  ProduceRecord,
  RawBatch,
  RawRecord,
  StartAt,
  Subscription,
} from "./types.js";
