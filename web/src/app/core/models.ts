// Mirrors internal/kafka/models.go. Keep in sync (later: generate from OpenAPI).

export interface Broker {
  id: number;
  host: string;
  port: number;
  rack?: string;
}

export interface ClusterInfo {
  name: string;
  clusterId: string;
  controllerId: number;
  brokers: Broker[];
  topicCount: number;
}

export interface TopicSummary {
  name: string;
  internal: boolean;
  partitions: number;
  replicationFactor: number;
  messages: number;
  underReplicated: number;
  offlinePartitions: number;
}

export interface Partition {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  startOffset: number;
  endOffset: number;
}

export interface ConfigEntry {
  name: string;
  value: string;
  source: string;
  sensitive: boolean;
  isDefault: boolean;
}

export interface TopicDetail extends TopicSummary {
  partitionList: Partition[];
  configs: ConfigEntry[] | null;
}

export interface GroupLag {
  topic: string;
  partition: number;
  committed: number;
  end: number;
  lag: number;
  memberId?: string;
}

export interface ConsumerGroup {
  name: string;
  state: string;
  protocolType: string;
  members: number;
  topics: string[];
  totalLag: number;
  lag?: GroupLag[];
  error?: string;
}

export interface Header {
  key: string;
  value: string;
}

export interface KafkaMessage {
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers?: Header[];
  keyEncoding?: 'utf8' | 'base64';
  valueEncoding?: 'utf8' | 'base64';
  valueSize: number;
  truncated?: boolean;
  keySchema?: SchemaRef;
  valueSchema?: SchemaRef;
}

export interface ProduceRequest {
  key: string | null;
  value: string | null;
  headers: Header[];
  partition?: number;
}

export interface ProduceResult {
  topic: string;
  partition: number;
  offset: number;
  timestamp: string;
}

export interface CreateTopicRequest {
  name: string;
  /** Omitted = broker default (num.partitions). */
  partitions?: number;
  /** Omitted = broker default (default.replication.factor). */
  replicationFactor?: number;
  configs?: Record<string, string>;
}

export interface CreateTopicResult {
  name: string;
  id?: string;
  partitions: number;
  replicationFactor: number;
}

export interface TopicConfigChange {
  set?: Record<string, string>;
  reset?: string[];
  /** Topic name; required by the server for data-destroying configs. */
  confirm?: string;
}

/** Configs that can delete existing messages (kept in sync with the server's list). */
export const DESTRUCTIVE_CONFIGS = new Set([
  'retention.ms',
  'retention.bytes',
  'local.retention.ms',
  'local.retention.bytes',
  'cleanup.policy',
  'delete.retention.ms',
  'remote.storage.enable',
]);

export function isDestructiveConfig(name: string): boolean {
  return DESTRUCTIVE_CONFIGS.has(name.trim().toLowerCase());
}

export interface PurgeResult {
  partitions: number;
  deleted: number;
}

export type ResetStrategy = 'earliest' | 'latest' | 'timestamp' | 'offset' | 'shift';

export interface ResetOffsetsRequest {
  topic: string;
  partitions?: number[];
  to: ResetStrategy;
  timestamp?: number;
  offset?: number;
  dryRun: boolean;
}

export interface OffsetChange {
  partition: number;
  /** Shift across all partitions: no commit to shift from, left unchanged. */
  skipped?: boolean;
  current: number;
  target: number;
  start: number;
  end: number;
  lagBefore: number;
  lagAfter: number;
}

export interface ResetOffsetsResult {
  group: string;
  topic: string;
  state: string;
  members: number;
  active: boolean;
  applied: boolean;
  changes: OffsetChange[];
}

export interface BuildInfo {
  version: string;
  commit?: string;
  buildDate?: string;
  dirty?: boolean;
}

export interface SchemaRef {
  id: number;
  type?: 'AVRO' | 'PROTOBUF' | 'JSON';
  /** Set when the schema could not be fetched or the payload did not match. */
  error?: string;
}

export interface Health {
  status: string;
  cluster: string;
  readOnly: boolean;
  schemaRegistry?: boolean;
  build?: BuildInfo;
}

export interface BrowseResult {
  messages: KafkaMessage[];
  scanned: number;
  complete: boolean;
}

export interface FilterError {
  error: string;
  column: number;
}

export interface SearchProgress {
  scanned: number;
  position: number;
  total: number;
  matched: number;
}

export type SearchMode = 'newest' | 'range' | 'all';

export interface SearchParams {
  mode: SearchMode;
  filter?: string;
  partition?: number | null;
  limit: number;
  scan?: number;
  from?: string; // RFC 3339
  to?: string;
}

export type SearchEvent =
  | { type: 'start'; total: number }
  | { type: 'batch'; messages: KafkaMessage[]; progress: SearchProgress }
  | { type: 'done'; reason: 'finished' | 'limit' | 'timeout' | 'incomplete'; progress: SearchProgress; elapsedMs: number }
  | { type: 'failure'; error: string };

export type LiveEvent =
  | { type: 'ready' }
  | { type: 'batch'; messages: KafkaMessage[]; dropped: number; scanned: number }
  | { type: 'reconnecting' }
  | { type: 'failure'; error: string };
