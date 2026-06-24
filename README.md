# 📊 Real-Time Crypto Market Monitoring System

[![EFREI](https://img.shields.io/badge/EFREI-Master%201%20Data%20Engineering%20%26%20IA-blue)](https://www.efrei.fr)
[![Node.js](https://img.shields.io/badge/Node.js-16%2B-green)](https://nodejs.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

**Un pipeline complet de stream processing pour monitoring de données crypto en temps réel.**

---

## 📌 Table des matières

- [Vue d'ensemble](#vue-densemble)
- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Lancement](#lancement)
- [Utilisation](#utilisation)
- [Structure du projet](#structure-du-projet)
- [Technologies](#technologies)
- [Résultats](#résultats)
- [Troubleshooting](#troubleshooting)
- [Contribution](#contribution)

---

## 🎯 Vue d'ensemble

### Qu'est-ce que c'est ?

Un **système de traitement de données en temps réel** qui :

✅ **Récupère** les données de prix BTC/USDT depuis Binance WebSocket  
✅ **Envoie** les trades à Kafka pour le buffering  
✅ **Traite** les données pour calculer des métriques toutes les 500ms  
✅ **Stocke** les données dans MongoDB  
✅ **Affiche** les résultats en direct sur un dashboard avec WebSocket  
✅ **Détecte** les anomalies (volumes anormaux)  

### Cas d'usage

- 📈 Monitoring en temps réel du marché crypto
- ⚠️ Détection d'anomalies et transactions suspectes
- 📊 Analyse de volatilité
- 🤖 Signaux pour trading algorithmique
- 💰 Surveillance de la liquidité

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  Binance WebSocket (Real-time trades)                     │
│           │                                                │
│           ▼                                                │
│  ┌────────────────┐                                       │
│  │   PRODUCER     │ ──► Kafka Topic ──► ┌──────────────┐ │
│  │  (producer.js) │                    │  PROCESSOR   │ │
│  └────────────────┘                    │(processor.js)│ │
│                                         └──────┬───────┘ │
│                                                │          │
│                                                ▼          │
│                                           MongoDB         │
│                                                │          │
│                                                ▼          │
│                                    ┌─────────────────────┐│
│                                    │  WebSocket Server   ││
│                                    │(websocket-server.js)││
│                                    └──────────┬──────────┘│
│                                               │           │
│                                               ▼           │
│                                      Dashboard (Live)     │
│                                    (public/index.html)    │
│                                                           │
└─────────────────────────────────────────────────────────────┘
```

### Flux de données

```
1. PRODUCER
   ├─ Se connecte à Binance WebSocket
   ├─ Reçoit les trades (price, quantity, timestamp)
   └─ Envoie chaque trade à Kafka "crypto.trades.raw"

2. KAFKA
   ├─ Buffering des messages
   └─ Mise à disposition pour les consumers

3. PROCESSOR
   ├─ Consomme les trades depuis Kafka
   ├─ Stocke les trades bruts dans MongoDB
   ├─ Calcule les métriques (fenêtre 5 min)
   │  ├─ Prix moyen
   │  ├─ Volume total
   │  ├─ Nombre de trades
   │  ├─ Anomalies (volume > seuil)
   │  └─ Min/Max du prix
   └─ Stocke les métriques dans MongoDB (500ms)

4. WEBSOCKET SERVER
   ├─ Écoute MongoDB en polling
   ├─ Détecte les nouvelles métriques
   └─ Envoie aux clients WebSocket connectés

5. DASHBOARD
   ├─ Se connecte au WebSocket Server
   ├─ Affiche les métriques en temps réel
   ├─ Graphique avec Chart.js
   └─ Alertes et notifications
```

---

## 📋 Prérequis

### Installation requise

- **Docker & Docker Compose** ([installer](https://www.docker.com/products/docker-desktop))
- **Node.js 16+** ([installer](https://nodejs.org/))
- **npm** (fourni avec Node.js)
- **Git** ([installer](https://git-scm.com/))

### Vérifier l'installation

```bash
# Vérifier Node.js
node --version  # v16.0.0 ou plus

# Vérifier npm
npm --version   # 7.0.0 ou plus

# Vérifier Docker
docker --version
docker-compose --version
```

---

## 🚀 Installation

### 1️⃣ Cloner le repository

```bash
git clone https://github.com/cheryle/Projet_real_time.git
cd Projet_real_time
```

### 2️⃣ Installer les dépendances Node.js

```bash
npm install
```

Cela installe :
- `kafkajs` - Client Kafka
- `mongodb` - Driver MongoDB
- `express` - Serveur web
- `ws` - WebSocket

### 3️⃣ Lancer l'infrastructure Docker

```bash
# Démarrer Kafka, Zookeeper et MongoDB
docker-compose up -d

# Vérifier que tout est lancé
docker ps
```

Tu devrais voir 3 conteneurs actifs :
- `projetrealtime-kafka-1`
- `projetrealtime-mongodb-1`
- `projetrealtime-zookeeper-1`

### 4️⃣ (Optionnel) Créer le dossier public

```bash
mkdir -p public
```

Si le dossier n'existe pas, il sera créé automatiquement.

---

## ⚡ Lancement

### Démarrer le pipeline complet

Tu dois lancer **4 commandes dans 4 terminaux différents** :

#### **Terminal 0 : Infrastructure (une seule fois)**

```bash
docker-compose up -d
```

✅ Lance Kafka, Zookeeper et MongoDB en arrière-plan.

---

#### **Terminal 1 : Producer (Binance → Kafka)**

```bash
node producer.js
```

**Logs attendus :**
```
✅ Producer connecté à Kafka
📨 Trade produit: 62706.02 USDT | 0.09 BTC
📨 Trade produit: 62707.15 USDT | 0.12 BTC
```

---

#### **Terminal 2 : Processor (Kafka → MongoDB)**

```bash
node processor.js
```

**Logs attendus :**
```
✅ Connecté à MongoDB
✅ Consumer Kafka connecté
📊 Métriques | Prix: 62694.85$ | Volume: 3.4 BTC | Trades: 1474
```

---

#### **Terminal 3 : WebSocket Server (MongoDB → Dashboard)**

```bash
node websocket-server.js
```

**Logs attendus :**
```
✅ Connecté à MongoDB
🚀 Serveur sur http://localhost:3000
🔗 Client connecté | Clients actifs: 1
📡 Métrique pushée à 1 client(s)
```

---

### 🌐 Accéder au Dashboard

Ouvre ton navigateur :

```
http://localhost:3000
```

Tu verras le dashboard avec :
- 📊 6 cartes de métriques
- 📈 Graphique temps réel
- ⚠️ Anomalies détectées
- ⚡ Alertes temps réel
- 📋 Trades récents
- 🔧 Santé du pipeline

---

## 💻 Utilisation

### Modifier le seuil d'anomalies

**Pour tester avec différents seuils :**

1. Ouvre `processor.js`
2. Change cette ligne :
   ```javascript
   const ANOMALY_THRESHOLD = 1;  // Modifie cette valeur (0.1, 0.5, 50, etc.)
   ```

3. Sauvegarde et redémarre le processor :
   ```bash
   Ctrl+C
   node processor.js
   ```

4. Recharge le dashboard (F5)

✅ Le seuil se met à jour automatiquement !

### Accès multi-clients

**Pour partager le dashboard avec un collègue :**

1. Trouve ton adresse IP locale :
   ```bash
   # Mac/Linux
   ifconfig | grep "inet " | grep -v 127.0.0.1
   
   # Windows
   ipconfig
   ```

2. Note l'adresse IP (exemple : `192.168.0.43`)

3. Modifie `websocket-server.js` :
   ```javascript
   server.listen(3000, '0.0.0.0', () => {
     console.log('🚀 Serveur sur http://192.168.0.43:3000');
   });
   ```

4. Redémarre le WebSocket Server

5. Partage le lien : `http://192.168.0.43:3000`

✅ Ton collègue reçoit les mêmes données en temps réel !

Le terminal affichera :
```
📡 Métrique pushée à 2 client(s)
```

---

## 📁 Structure du projet

```
Projet_real_time/
│
├── producer.js                 # Source de données (Binance WebSocket)
├── processor.js                # Traitement (agrégation, anomalies)
├── websocket-server.js         # Serveur WebSocket + API REST
│
├── public/
│   └── index.html             # Dashboard frontend (HTML + CSS + JS)
│
├── docker-compose.yml         # Configuration Docker (Kafka, Zookeeper, MongoDB)
├── package.json               # Dépendances Node.js
│
├── README.md                  # Ce fichier
├── .gitignore                 # Fichiers ignorés par Git
└── LICENSE                    # Licence MIT
```

---

## 🛠️ Technologies

| Technologie | Usage |
|-------------|-------|
| **Binance WebSocket** | Source de données crypto en temps réel |
| **Apache Kafka** | Message broker pour le buffering |
| **MongoDB** | Base de données NoSQL (trades + métriques) |
| **Node.js** | Runtime JavaScript backend |
| **Express.js** | Serveur web REST |
| **WebSocket (ws)** | Communication temps réel bidirectionnelle |
| **Chart.js** | Visualisation graphique côté client |
| **Docker & Compose** | Containerization et orchestration |

---

## 📊 Résultats

### Performance mesurée

| Métrique | Valeur |
|----------|--------|
| **Débit** | ~20 trades/seconde |
| **Trades par fenêtre** | 1474 (5 min) |
| **Latence pipeline** | ~150ms |
| **Calcul métriques** | Toutes les 500ms |
| **Fenêtre glissante** | 5 minutes |

### Métriques affichées

- ✅ Prix BTC/USDT actuel
- ✅ Volume total (5 min)
- ✅ Moyenne mobile
- ✅ Nombre de trades
- ✅ Anomalies détectées
- ✅ Range (Min/Max)
- ✅ Trades récents (10)
- ✅ Santé du pipeline

---

## 🔧 Troubleshooting

### ❌ Docker containers ne démarrent pas

```bash
# Vérifier les conteneurs
docker ps -a

# Voir les logs
docker logs projetrealtime-kafka-1

# Redémarrer
docker-compose down
docker-compose up -d
```

### ❌ Dashboard vide / pas de données

**Vérifier :**

1. Les 3 terminaux tournent-ils ?
   ```bash
   # Terminal 1: node producer.js
   # Terminal 2: node processor.js
   # Terminal 3: node websocket-server.js
   ```

2. Docker est-il lancé ?
   ```bash
   docker ps
   ```

3. MongoDB a données ?
   ```bash
   docker exec -it projetrealtime-mongodb-1 mongosh
   > show dbs
   > use crypto_monitor
   > show collections
   ```

### ❌ Erreur "Connection refused"

```bash
# Redémarrer Docker
docker-compose down
docker-compose up -d

# Attendre 10 secondes
sleep 10

# Relancer les services
node producer.js
node processor.js
node websocket-server.js
```

### ❌ Collegue ne peut pas accéder au dashboard

1. **Même WiFi ?** Vérifier qu'il est sur le même réseau
2. **Adresse IP correcte ?** Vérifier avec `ifconfig`
3. **WebSocket Server écoute sur 0.0.0.0 ?** 
   ```javascript
   server.listen(3000, '0.0.0.0', ...)
   ```
4. **Firewall ?** Peut-être bloquer le port 3000

---

## 📚 Documentation détaillée

Pour plus de détails sur le code et l'architecture, consulte :

- **[DOCUMENTATION_COMPLETE.md](./DOCUMENTATION_COMPLETE.md)** - Explications détaillées de chaque composant
- **[PRESENTATION_EFREI.pptx](./PRESENTATION_EFREI.pptx)** - Présentation du projet

---

## 🤝 Contribution

Les contributions sont bienvenues ! 

### Pour contribuer :

1. Fork le repository
2. Crée une branche (`git checkout -b feature/AmazingFeature`)
3. Commit tes changements (`git commit -m 'Add AmazingFeature'`)
4. Push sur la branche (`git push origin feature/AmazingFeature`)
5. Ouvre une Pull Request

### Idées d'amélioration

- [ ] Ajouter l'authentification API Binance (au lieu du WebSocket public)
- [ ] Intégrer Prometheus + Grafana pour le monitoring avancé
- [ ] Ajouter un modèle ML pour la prédiction de prix
- [ ] Implémenter la réplication Kafka
- [ ] Ajouter des tests unitaires (Jest)
- [ ] Déployer sur AWS/GCP
- [ ] Support de plusieurs paires crypto (ETHUSDT, etc.)
- [ ] API GraphQL

---

## 📄 License

Ce projet est sous licence **MIT**. Voir [LICENSE](LICENSE) pour plus de détails.

---

## 👤 Auteur

**Cheryle Awa Efreni**  
Master 1 Data Engineering & IA  
EFREI Paris | Juin 2026

---

## 📞 Support

Si tu as des questions ou des problèmes :

1. Consulte le [Troubleshooting](#troubleshooting)
2. Lis la [Documentation complète](./DOCUMENTATION_COMPLETE.md)
3. Ouvre une [Issue](../../issues) sur GitHub

---

## 🌟 Remerciements

- 🎓 EFREI Paris pour la formation
- 📊 Binance pour l'API WebSocket
- 🐳 Docker pour la containerization
- ⚙️ Apache Kafka pour le message broking
- 💾 MongoDB pour la persistance

---

<div align="center">

**⭐ Si ce projet t'a aidé, n'hésite pas à mettre une star ! ⭐**

[⬆ Retour au début](#-real-time-crypto-market-monitoring-system)

</div>
