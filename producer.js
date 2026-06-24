const { Kafka } = require('kafkajs');
const WebSocket = require('ws');

const kafka = new Kafka({
  clientId: 'binance-producer',
  brokers: ['localhost:29092']
});

const producer = kafka.producer();

async function produceFromBinance() {
  await producer.connect();
  console.log('✅ Producer connecté à Kafka');

  const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@trade');

  ws.on('open', () => {
    console.log('🔗 Connecté à Binance WebSocket');
  });

  ws.on('message', async (data) => {
    try {
      const trade = JSON.parse(data);

      // Envoie le trade brut dans Kafka
      await producer.send({
        topic: 'crypto.trades.raw',
        messages: [
          {
            key: 'BTC/USDT',
            value: JSON.stringify({
              price: trade.p,
              quantity: trade.q,
              timestamp: trade.T,
              isMaker: trade.m,
              tradeId: trade.a
            })
          }
        ]
      });

      console.log(`📨 Trade produit: ${trade.p} USDT | ${trade.q} BTC`);
    } catch (err) {
      console.error('Erreur:', err.message);
    }
  });

  ws.on('error', (err) => {
    console.error('❌ Erreur Binance:', err.message);
  });

  ws.on('close', () => {
    console.log('⚫ Connexion Binance fermée');
  });
}

produceFromBinance().catch(console.error);