// config.ts - Configuration centralisée du projet

import { KafkaConfig, MongoConfig, ServerConfig, AggregatorConfig } from './types';

/**
 * Configuration Kafka
 */
export const kafkaConfig: KafkaConfig = {
  clientId: 'crypto-monitor',
  brokers: process.env.KAFKA_BROKERS?.split(',') || ['localhost:29092'],
  topic: process.env.KAFKA_TOPIC || 'crypto.trades.raw'
};

/**
 * Configuration MongoDB
 */
export const mongoConfig: MongoConfig = {
  uri: process.env.MONGO_URI || 'mongodb://localhost:27018',
  dbName: 'crypto_monitor',
  collections: {
    trades: 'trades',
    metrics: 'metrics'
  }
};

/**
 * Configuration du serveur WebSocket
 */
export const serverConfig: ServerConfig = {
  port: parseInt(process.env.PORT || '3000', 10),
  host: process.env.HOST || '0.0.0.0',
  mongoUri: mongoConfig.uri
};

/**
 * Configuration de l'agrégateur
 */
export const aggregatorConfig: AggregatorConfig = {
  windowMs: 5 * 60 * 1000,              // 5 minutes
  anomalyThreshold: parseFloat(
    process.env.ANOMALY_THRESHOLD || '1'
  ),
  metricsInterval: 500                  // 500ms
};

/**
 * Configuration Binance
 */
export const binanceConfig = {
  symbol: 'BTCUSDT',
  stream: 'btcusdt@trade',
  wsUrl: 'wss://stream.binance.com:9443/ws'
};

/**
 * Configuration logging
 */
export const logConfig = {
  level: process.env.LOG_LEVEL || 'info',
  format: process.env.LOG_FORMAT || 'text'
};

/**
 * Affiche la configuration (sans les secrets)
 */
export function logConfiguration(): void {
  console.log('⚙️  Configuration chargée:');
  console.log(`  Kafka: ${kafkaConfig.brokers.join(', ')}`);
  console.log(`  MongoDB: ${mongoConfig.uri}`);
  console.log(`  Server: ${serverConfig.host}:${serverConfig.port}`);
  console.log(`  Anomaly Threshold: ${aggregatorConfig.anomalyThreshold} BTC`);
  console.log(`  Window: ${aggregatorConfig.windowMs / 1000 / 60} minutes`);
}

/**
 * Valide la configuration
 */
export function validateConfiguration(): boolean {
  const errors: string[] = [];

  if (!kafkaConfig.brokers || kafkaConfig.brokers.length === 0) {
    errors.push('KAFKA_BROKERS est requis');
  }

  if (!mongoConfig.uri) {
    errors.push('MONGO_URI est requis');
  }

  if (aggregatorConfig.anomalyThreshold <= 0) {
    errors.push('ANOMALY_THRESHOLD doit être > 0');
  }

  if (errors.length > 0) {
    console.error('❌ Erreurs de configuration:');
    errors.forEach(err => console.error(`  - ${err}`));
    return false;
  }

  return true;
}
