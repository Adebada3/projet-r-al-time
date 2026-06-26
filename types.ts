// types.ts - Définitions des types TypeScript

/**
 * Trade brut depuis Binance
 */
export interface Trade {
  price: number;
  quantity: number;
  timestamp: number;
  symbol?: string;
}

/**
 * Métrique agrégée
 */
export interface Metric {
  timestamp: Date;
  avgPrice: number;
  totalVolume: number;
  tradeCount: number;
  minPrice: number;
  maxPrice: number;
  lastPrice: number;
  priceChange: number;
  anomalyCount: number;
  anomalies: Trade[];
  anomalyThreshold: number;
  recentTrades: Trade[];
}

/**
 * Configuration de l'agrégateur
 */
export interface AggregatorConfig {
  windowMs: number; // Taille de la fenêtre en ms
  anomalyThreshold: number; // Seuil d'anomalie en BTC
  metricsInterval: number; // Intervalle de calcul des métriques
}

/**
 * Configuration Kafka
 */
export interface KafkaConfig {
  clientId: string;
  brokers: string[];
  topic: string;
}

/**
 * Configuration MongoDB
 */
export interface MongoConfig {
  uri: string;
  dbName: string;
  collections: {
    trades: string;
    metrics: string;
  };
}

/**
 * Configuration du serveur WebSocket
 */
export interface ServerConfig {
  port: number;
  host: string;
  mongoUri: string;
}

/**
 * Message WebSocket
 */
export interface WebSocketMessage {
  type: 'METRIC_UPDATE' | 'ERROR' | 'PING';
  data: Metric | Error | null;
  timestamp: Date;
}

/**
 * Statut du pipeline
 */
export interface PipelineStatus {
  producer: boolean;
  kafka: boolean;
  processor: boolean;
  mongodb: boolean;
  websocket: boolean;
  connectedClients: number;
}
