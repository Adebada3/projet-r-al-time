const { Kafka } = require('kafkajs');
const { MongoClient } = require('mongodb');

const ANOMALY_THRESHOLD = 50;  // ✅ À MODIFIER ICI (1, 0.5, 0.1, etc.)

const kafka = new Kafka({
  clientId: 'stream-processor',
  brokers: ['localhost:29092']
});

const mongoClient = new MongoClient('mongodb://localhost:27018');
let db;

class MetricsAggregator {
  constructor(windowMs = 5 * 60 * 1000) {
    this.trades = [];
    this.windowMs = windowMs;
    this.lastPrice = null;
  }

  addTrade(trade) {
    const now = Date.now();
    this.trades.push({
      ...trade,
      price: parseFloat(trade.price),
      quantity: parseFloat(trade.quantity),
      timestamp: now
    });

    // Nettoie les trades hors fenêtre (garde que les 5 dernières minutes)
    this.trades = this.trades.filter(t => now - t.timestamp < this.windowMs);
    this.lastPrice = parseFloat(trade.price);
  }

  calculateMetrics() {
    if (this.trades.length === 0) return null;

    const prices = this.trades.map(t => t.price);
    const quantities = this.trades.map(t => t.quantity);
    const avgPrice = prices.reduce((a, b) => a + b) / prices.length;
    const totalVolume = quantities.reduce((a, b) => a + b);

    // ✅ Anomalies : volume > ANOMALY_THRESHOLD
    const anomalies = this.trades.filter(t => t.quantity > ANOMALY_THRESHOLD);
    
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const priceChange = ((this.lastPrice - avgPrice) / avgPrice * 100).toFixed(2);

    return {
      timestamp: new Date(),
      avgPrice: parseFloat(avgPrice.toFixed(2)),
      totalVolume: parseFloat(totalVolume.toFixed(2)),
      tradeCount: this.trades.length,
      minPrice: parseFloat(minPrice.toFixed(2)),
      maxPrice: parseFloat(maxPrice.toFixed(2)),
      lastPrice: this.lastPrice,
      priceChange: parseFloat(priceChange),
      anomalyCount: anomalies.length,
      anomalies: anomalies.slice(-10),
      anomalyThreshold: ANOMALY_THRESHOLD,  // ✅ ENVOIE LE SEUIL
      recentTrades: this.trades.slice(-20).reverse()  // ✅ 20 DERNIERS TRADES
    };
  }
}

async function runProcessor() {
  try {
    await mongoClient.connect();
    db = mongoClient.db('crypto_monitor');
    console.log('✅ Connecté à MongoDB');

    const tradesCollection = db.collection('trades');
    const metricsCollection = db.collection('metrics');

    await tradesCollection.createIndex({ timestamp: -1 });
    await metricsCollection.createIndex({ timestamp: -1 });

    const consumer = kafka.consumer({ groupId: 'stream-processor-group' });
    await consumer.connect();
    console.log('✅ Consumer Kafka connecté');

    await consumer.subscribe({ topic: 'crypto.trades.raw', fromBeginning: false });

    const aggregator = new MetricsAggregator(5 * 60 * 1000);
    let messageCount = 0;

    // Sauvegarde les métriques toutes les 500ms
    setInterval(async () => {
      const metrics = aggregator.calculateMetrics();
      if (metrics) {
        await metricsCollection.insertOne(metrics);
        console.log(`📊 Métriques | Prix: ${metrics.avgPrice}$ | Volume: ${metrics.totalVolume} BTC | Trades: ${metrics.tradeCount} | Anomalies: ${metrics.anomalyCount}`);
      }
    }, 500);

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const trade = JSON.parse(message.value.toString());
          await tradesCollection.insertOne({
            ...trade,
            storedAt: new Date()
          });
          aggregator.addTrade(trade);
          messageCount++;

          if (messageCount % 100 === 0) {
            console.log(`✅ ${messageCount} trades traités`);
          }
        } catch (err) {
          console.error('Erreur:', err.message);
        }
      }
    });
  } catch (err) {
    console.error('❌ Erreur:', err);
  }
}

runProcessor().catch(console.error);