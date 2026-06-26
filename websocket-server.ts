// websocket-server.ts - Version TypeScript du websocket-server.js

import express, { Express, Request, Response } from 'express';
import http from 'http';
import WebSocket from 'ws';
import { MongoClient, Collection } from 'mongodb';
import path from 'path';
import fs from 'fs';
import * as dotenv from 'dotenv';

dotenv.config();

// Configuration
const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27018';

// Types
interface Metric {
  _id?: string;
  timestamp: Date;
  avgPrice: number;
  totalVolume: number;
  tradeCount: number;
  minPrice: number;
  maxPrice: number;
  lastPrice: number;
  priceChange: number;
  anomalyCount: number;
  anomalies: any[];
  anomalyThreshold: number;
  recentTrades: any[];
}

interface WebSocketMessage {
  type: 'METRIC_UPDATE' | 'ERROR' | 'PING';
  data: Metric | { error: string } | null;
  timestamp: Date;
}

/**
 * Server principal
 */
class MetricsServer {
  private app: Express;
  private server: http.Server;
  private wss: WebSocket.Server;
  private mongoClient: MongoClient;
  private metricsCollection: Collection | null = null;

  constructor() {
    this.app = express();
    this.server = http.createServer(this.app);
    this.wss = new WebSocket.Server({ server: this.server });
    this.mongoClient = new MongoClient(MONGO_URI);

    this.setupRoutes();
    this.setupWebSocket();
  }

  /**
   * Configure les routes HTTP
   */
  private setupRoutes(): void {
    // Crée le dossier public s'il n'existe pas
    const publicDir = path.join(__dirname, 'public');
    if (!fs.existsSync(publicDir)) {
      fs.mkdirSync(publicDir, { recursive: true });
    }

    // Middleware
    this.app.use(express.static('public'));

    // Routes
    this.app.get('/', (_req: Request, res: Response) => {
      res.sendFile(path.join(__dirname, 'public', 'index.html'));
    });

    this.app.get('/api/metrics/latest', async (_req: Request, res: Response) => {
      try {
        if (!this.metricsCollection) {
          res.status(503).json({ error: 'MongoDB not connected' });
          return;
        }

        const latest = await this.metricsCollection.findOne<Metric>(
          {},
          { sort: { timestamp: -1 } }
        );

        res.json(latest || {});
      } catch (err) {
        res.status(500).json({ error: (err as Error).message });
      }
    });

    this.app.get('/api/metrics/history', async (_req: Request, res: Response) => {
      try {
        if (!this.metricsCollection) {
          res.status(503).json({ error: 'MongoDB not connected' });
          return;
        }

        const history = await this.metricsCollection
          .find<Metric>({})
          .sort({ timestamp: -1 })
          .limit(100)
          .toArray();

        res.json(history);
      } catch (err) {
        res.status(500).json({ error: (err as Error).message });
      }
    });

    this.app.get('/api/status', (_req: Request, res: Response) => {
      res.json({
        status: 'running',
        connectedClients: this.wss.clients.size,
        timestamp: new Date()
      });
    });
  }

  /**
   * Configure les WebSockets
   */
  private setupWebSocket(): void {
    this.wss.on('connection', (ws: WebSocket) => {
      console.log(
        `🔗 Client connecté | Clients actifs: ${this.wss.clients.size}`
      );

      ws.on('close', () => {
        console.log(
          `❌ Client déconnecté | Clients actifs: ${this.wss.clients.size}`
        );
      });

      ws.on('error', (err: Error) => {
        console.error('❌ Erreur WebSocket:', err.message);
      });

      ws.on('message', (data: string) => {
        try {
          const message = JSON.parse(data);
          if (message.type === 'PING') {
            ws.send(JSON.stringify({ type: 'PONG', timestamp: new Date() }));
          }
        } catch (err) {
          console.error('❌ Erreur message:', (err as Error).message);
        }
      });
    });
  }

  /**
   * Démarre le polling MongoDB
   */
  private startMetricsWatching(): void {
    let lastTimestamp = new Date(0);

    // Poll MongoDB toutes les 500ms
    setInterval(async () => {
      try {
        if (!this.metricsCollection) return;

        const newMetrics = await this.metricsCollection.findOne<Metric>(
          { timestamp: { $gt: lastTimestamp } },
          { sort: { timestamp: -1 } }
        );

        if (newMetrics) {
          lastTimestamp = newMetrics.timestamp;

          // Envoie à tous les clients connectés
          this.broadcastMetric(newMetrics);
        }
      } catch (err) {
        console.error(
          '❌ Erreur MongoDB polling:',
          (err as Error).message
        );
      }
    }, 500);
  }

  /**
   * Envoie une métrique à tous les clients
   */
  private broadcastMetric(metric: Metric): void {
    const message: WebSocketMessage = {
      type: 'METRIC_UPDATE',
      data: metric,
      timestamp: new Date()
    };

    this.wss.clients.forEach((client: WebSocket) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(message));
      }
    });

    console.log(`📡 Métrique pushée à ${this.wss.clients.size} client(s)`);
  }

  /**
   * Démarre le serveur
   */
  public async start(): Promise<void> {
    try {
      // Connecter à MongoDB
      await this.mongoClient.connect();
      const db = this.mongoClient.db('crypto_monitor');
      this.metricsCollection = db.collection('metrics');

      console.log('✅ Connecté à MongoDB');

      // Démarrer le polling
      this.startMetricsWatching();

      // Démarrer le serveur HTTP
      this.server.listen(PORT, HOST, () => {
        console.log(`🚀 Serveur sur http://${HOST}:${PORT}`);
        console.log(`📡 WebSocket prêt pour les clients`);
      });
    } catch (err) {
      console.error('❌ Erreur démarrage serveur:', err);
      process.exit(1);
    }
  }

  /**
   * Arrête le serveur
   */
  public async stop(): Promise<void> {
    try {
      await this.mongoClient.close();
      this.server.close();
      console.log('✅ Serveur arrêté');
    } catch (err) {
      console.error('❌ Erreur arrêt serveur:', err);
    }
  }
}

/**
 * Point d'entrée
 */
async function main(): Promise<void> {
  const server = new MetricsServer();

  try {
    await server.start();
  } catch (err) {
    console.error('❌ Impossible de démarrer le serveur:', err);
    process.exit(1);
  }

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\n⏹️ Arrêt du serveur...');
    await server.stop();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('❌ Erreur fatale:', err);
  process.exit(1);
});

export { MetricsServer };
