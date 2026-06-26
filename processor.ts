// processor.ts - Stream processor avec TypeScript

import { Kafka } from 'kafkajs';
import { MongoClient, Db, Collection } from 'mongodb';
import { Trade, Metric, AggregatorConfig } from './types';

const ANOMALY_THRESHOLD = 1; // BTC

/**
 * Classe responsable de l'agrégation des métriques
 */
class MetricsAggregator {
  private trades: Trade[] = [];
  private windowMs: number;
  private lastPrice: number = 0;
  private anomalyThreshold: number;

  constructor(config: AggregatorConfig) {
    this.windowMs = config.windowMs;
    this.anomalyThreshold = config.anomalyThreshold;
  }

  /**
   * Ajoute un trade à la fenêtre glissante
   */
  public addTrade(trade: Trade): void {
    const now = Date.now();
    
    this.trades.push({
      ...trade,
      price: parseFloat(trade.price.toString()),
      quantity: parseFloat(trade.quantity.toString()),
      timestamp: now
    });

    // Nettoie les trades hors fenêtre (garde que les 5 dernières minutes)
    this.trades = this.trades.filter(t => now - t.timestamp < this.windowMs);
    this.lastPrice = parseFloat(trade.price.toString());
  }

  /**
   * Calcule les métriques agrégées
   */
  public calculateMetrics(): Metric | null {
    if (this.trades.length === 0) return null;

    const prices = this.trades.map(t => t.price);
    const quantities = this.trades.map(t => t.quantity);

    const avgPrice = prices.reduce((a, b) => a + b) / prices.length;
    const totalVolume = quantities.reduce((a, b) => a + b);
    const anomalies = this.trades.filter(t => t.quantity > this.anomalyThreshold);
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const priceChange = ((this.lastPrice - avgPrice) / avgPrice * 100);

    return {
      timestamp: new Date(),
      avgPrice: parseFloat(avgPrice.toFixed(2)),
      totalVolume: parseFloat(totalVolume.toFixed(2)),
      tradeCount: this.trades.length,
      minPrice: parseFloat(minPrice.toFixed(2)),
      maxPrice: parseFloat(maxPrice.toFixed(2)),
      lastPrice: this.lastPrice,
      priceChange: parseFloat(priceChange.toFixed(2)),
      anomalyCount: anomalies.length,
      anomalies: anomalies.slice(-10),
      anomalyThreshold: this.anomalyThreshold,
      recentTrades: this.trades.slice(-20).reverse()
    };
  }

  /**
   * Obtient la taille actuelle de la fenêtre
   */
  public getWindowSize(): number {
    return this.trades.length;
  }
}

/**
 * Classe principal du Stream Processor
 */
class StreamProcessor {
  private kafka: Kafka;
  private mongoClient: MongoClient;
  private db: Db | null = null;
  private tradesCollection: Collection | null = null;
  private metricsCollection: Collection | null = null;
  private aggregator: MetricsAggregator;

  constructor(kafkaBrokers: string[], mongoUri: string, aggregatorConfig: AggregatorConfig) {
    this.kafka = new Kafka({
      clientId: 'stream-processor',
      brokers: kafkaBrokers,
      connectionTimeout: 10000,
      requestTimeout: 10000
    });

    this.mongoClient = new MongoClient(mongoUri);
    this.aggregator = new MetricsAggregator(aggregatorConfig);
  }

  /**
   * Connecte à MongoDB et Kafka
   */
  public async connect(): Promise<void> {
    try {
      // MongoDB connection
      await this.mongoClient.connect();
      this.db = this.mongoClient.db('crypto_monitor');
      this.tradesCollection = this.db.collection('trades');
      this.metricsCollection = this.db.collection('metrics');

      console.log('✅ Connecté à MongoDB');

      // Créer les index
      await this.createIndexes();

      // Kafka connection
      const consumer = this.kafka.consumer({ groupId: 'stream-processor-group' });
      await consumer.connect();
      console.log('✅ Consumer Kafka connecté');

      await consumer.subscribe({ topic: 'crypto.trades.raw', fromBeginning: false });

      // Démarrer le polling des métriques
      this.startMetricsPolling();

      // Consommer les messages
      await consumer.run({
        eachMessage: async ({ message }) => {
          await this.handleMessage(message);
        }
      });
    } catch (error) {
      console.error('❌ Erreur de connexion:', error);
      throw error;
    }
  }

  /**
   * Crée les index MongoDB
   */
  private async createIndexes(): Promise<void> {
    if (!this.tradesCollection || !this.metricsCollection) return;

    try {
      await this.tradesCollection.createIndex({ timestamp: -1 });
      await this.metricsCollection.createIndex({ timestamp: -1 });
      console.log('✅ Index MongoDB créés');
    } catch (error) {
      console.error('⚠️ Erreur création index:', error);
    }
  }

  /**
   * Traite un message Kafka
   */
  private async handleMessage(message: any): Promise<void> {
    try {
      const trade: Trade = JSON.parse(message.value?.toString() || '{}');

      // Valider les données
      if (!this.isValidTrade(trade)) {
        console.warn('⚠️ Trade invalide reçu:', trade);
        return;
      }

      // Stocker le trade brut
      if (this.tradesCollection) {
        await this.tradesCollection.insertOne({
          ...trade,
          storedAt: new Date()
        });
      }

      // Ajouter à l'agrégateur
      this.aggregator.addTrade(trade);
    } catch (error) {
      console.error('❌ Erreur traitement message:', error);
    }
  }

  /**
   * Valide un trade
   */
  private isValidTrade(trade: any): boolean {
    return (
      (typeof trade.price === 'string' || typeof trade.price === 'number') &&
      (typeof trade.quantity === 'string' || typeof trade.quantity === 'number')
    );
  }

  /**
   * Démarre le polling des métriques
   */
  private startMetricsPolling(): void {
    let messageCount = 0;

    setInterval(async () => {
      try {
        const metrics = this.aggregator.calculateMetrics();

        if (metrics && this.metricsCollection) {
          await this.metricsCollection.insertOne(metrics);
          messageCount++;

          console.log(
            `📊 Métriques | Prix: ${metrics.avgPrice}$ | ` +
            `Volume: ${metrics.totalVolume} BTC | ` +
            `Trades: ${metrics.tradeCount} | ` +
            `Anomalies: ${metrics.anomalyCount}`
          );

          if (messageCount % 100 === 0) {
            console.log(`✅ ${messageCount} métriques calculées`);
          }
        }
      } catch (error) {
        console.error('❌ Erreur calcul métriques:', error);
      }
    }, 500);
  }

  /**
   * Ferme la connexion
   */
  public async disconnect(): Promise<void> {
    try {
      await this.mongoClient.close();
      console.log('✅ Déconnecté de MongoDB');
    } catch (error) {
      console.error('❌ Erreur déconnexion:', error);
    }
  }
}

/**
 * Point d'entrée du processor
 */
async function main(): Promise<void> {
  const processor = new StreamProcessor(
    ['localhost:29092'],
    'mongodb://localhost:27018',
    {
      windowMs: 5 * 60 * 1000,        // 5 minutes
      anomalyThreshold: ANOMALY_THRESHOLD,
      metricsInterval: 500             // 500ms
    }
  );

  try {
    await processor.connect();
  } catch (error) {
    console.error('❌ Impossible de démarrer le processor:', error);
    process.exit(1);
  }

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n⏹️ Arrêt du processor...');
    await processor.disconnect();
    process.exit(0);
  });
}

// Démarrer le processor
main().catch(error => {
  console.error('Erreur fatale:', error);
  process.exit(1);
});

export { StreamProcessor, MetricsAggregator };
