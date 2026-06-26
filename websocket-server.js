const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const { MongoClient } = require('mongodb');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const mongoClient = new MongoClient('mongodb://localhost:27018');
let metricsCollection;

// Crée le dossier public s'il n'existe pas
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) {
  fs.mkdirSync(publicDir);
}

app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/api/metrics/latest', async (req, res) => {
  try {
    const latest = await metricsCollection
      .findOne({}, { sort: { timestamp: -1 } });
    res.json(latest || {});
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/metrics/history', async (req, res) => {
  try {
    const history = await metricsCollection
      .find({})
      .sort({ timestamp: -1 })
      .limit(100)
      .toArray();
    res.json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

wss.on('connection', (ws) => {
  console.log(`🔗 Client connecté | Clients actifs: ${wss.clients.size}`);
  
  ws.on('close', () => {
    console.log(`❌ Client déconnecté | Clients actifs: ${wss.clients.size}`);
  });

  ws.on('error', (err) => {
    console.error('Erreur WebSocket:', err.message);
  });
});

async function watchMetrics() {
  try {
    let lastTimestamp = new Date(0);

    // Poll MongoDB toutes les 500ms au lieu d'utiliser change streams
    setInterval(async () => {
      const newMetrics = await metricsCollection
        .findOne({ timestamp: { $gt: lastTimestamp } }, { sort: { timestamp: -1 } });

      if (newMetrics) {
        lastTimestamp = newMetrics.timestamp;

        // Envoie à tous les clients connectés
        wss.clients.forEach(client => {
          if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify({
              type: 'METRIC_UPDATE',
              data: newMetrics
            }));
          }
        });

        console.log(`📡 Métrique pushée à ${wss.clients.size} client(s)`);
      }
    }, 500);
  } catch (err) {
    console.error('Erreur MongoDB polling:', err.message);
  }
}

async function startServer() {
  try {
    await mongoClient.connect();
    metricsCollection = mongoClient.db('crypto_monitor').collection('metrics');
    console.log('✅ Connecté à MongoDB');

    server.listen(3000, '0.0.0.0', () => {
      console.log('🚀 Serveur sur http://192.168.0.43:3000');
    });

    await watchMetrics();
  } catch (err) {
    console.error('❌ Erreur:', err);
    process.exit(1);
  }
}

startServer();