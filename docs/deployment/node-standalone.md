# Node.js Standalone Deployment

This guide covers deploying ParqueDB as a standalone Node.js server without Cloudflare Workers.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Basic Server Setup](#basic-server-setup)
- [Storage Configuration](#storage-configuration)
- [Production Deployment](#production-deployment)
- [Docker Deployment](#docker-deployment)
- [Monitoring](#monitoring)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

---

## Overview

ParqueDB can run as a standalone Node.js application using the filesystem backend (`FsBackend`). This is ideal for:

- Development and testing
- Self-hosted deployments
- On-premises installations
- Integration with existing Node.js applications
- Environments without Cloudflare

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                     Node.js Server                          │
│                                                             │
│  ┌─────────────────┐    ┌──────────────────────────────┐   │
│  │   HTTP Server   │────│        ParqueDB              │   │
│  │   (Express/     │    │                              │   │
│  │   Fastify/Hono) │    │  ┌────────────────────────┐  │   │
│  └─────────────────┘    │  │      FsBackend         │  │   │
│                         │  │   (Filesystem I/O)     │  │   │
│                         │  └───────────┬────────────┘  │   │
│                         └──────────────┼───────────────┘   │
│                                        │                    │
│  ┌─────────────────────────────────────▼────────────────┐  │
│  │                  Local Filesystem                     │  │
│  │  ./data/                                              │  │
│  │  ├── posts/data.parquet                               │  │
│  │  ├── users/data.parquet                               │  │
│  │  └── indexes/...                                      │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Installation

```bash
npm install parquedb

# For TypeScript support
npm install -D typescript @types/node

# Optional: HTTP framework
npm install express  # or fastify, hono, etc.
```

---

## Basic Server Setup

### Minimal Example

```typescript
// server.ts
import { ParqueDB, FsBackend } from 'parquedb'
import { createServer } from 'node:http'

// Initialize ParqueDB with filesystem storage
const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// Create HTTP server
const server = createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host}`)

  try {
    // GET /posts - List posts
    if (url.pathname === '/posts' && req.method === 'GET') {
      const posts = await db.Posts.find({ status: 'published' })
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(posts))
      return
    }

    // GET /posts/:id - Get single post
    const postMatch = url.pathname.match(/^\/posts\/([^/]+)$/)
    if (postMatch && req.method === 'GET') {
      const post = await db.Posts.get(postMatch[1])
      if (!post) {
        res.writeHead(404, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify({ error: 'Not found' }))
        return
      }
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(post))
      return
    }

    // POST /posts - Create post
    if (url.pathname === '/posts' && req.method === 'POST') {
      let body = ''
      for await (const chunk of req) {
        body += chunk
      }
      const data = JSON.parse(body)
      const post = await db.Posts.create(data)
      res.writeHead(201, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify(post))
      return
    }

    // 404
    res.writeHead(404, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ error: 'Not found' }))
  } catch (error) {
    console.error('Error:', error)
    res.writeHead(500, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({
      error: error instanceof Error ? error.message : 'Internal error'
    }))
  }
})

const PORT = process.env.PORT || 3000
server.listen(PORT, () => {
  console.log(`ParqueDB server running on port ${PORT}`)
})
```

### Express.js Example

```typescript
// server.ts
import express from 'express'
import { ParqueDB, FsBackend } from 'parquedb'

const app = express()
app.use(express.json())

// Initialize ParqueDB
const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// REST API endpoints
app.get('/api/:collection', async (req, res) => {
  try {
    const { collection } = req.params
    const filter = req.query.filter ? JSON.parse(req.query.filter as string) : {}
    const limit = req.query.limit ? parseInt(req.query.limit as string) : 20

    const result = await db.collection(collection).find(filter, { limit })
    res.json(result)
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Query failed'
    })
  }
})

app.get('/api/:collection/:id', async (req, res) => {
  try {
    const { collection, id } = req.params
    const entity = await db.collection(collection).get(id)
    if (!entity) {
      return res.status(404).json({ error: 'Not found' })
    }
    res.json(entity)
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Query failed'
    })
  }
})

app.post('/api/:collection', async (req, res) => {
  try {
    const { collection } = req.params
    const entity = await db.collection(collection).create(req.body)
    res.status(201).json(entity)
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Create failed'
    })
  }
})

app.patch('/api/:collection/:id', async (req, res) => {
  try {
    const { collection, id } = req.params
    const result = await db.collection(collection).update(id, req.body)
    res.json(result)
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Update failed'
    })
  }
})

app.delete('/api/:collection/:id', async (req, res) => {
  try {
    const { collection, id } = req.params
    const result = await db.collection(collection).delete(id)
    res.json(result)
  } catch (error) {
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Delete failed'
    })
  }
})

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', timestamp: new Date().toISOString() })
})

const PORT = process.env.PORT || 3000
app.listen(PORT, () => {
  console.log(`ParqueDB server running on http://localhost:${PORT}`)
})
```

### Fastify Example

```typescript
// server.ts
import Fastify from 'fastify'
import { ParqueDB, FsBackend } from 'parquedb'

const fastify = Fastify({ logger: true })

// Initialize ParqueDB
const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// Register routes
fastify.get('/api/:collection', async (request, reply) => {
  const { collection } = request.params as { collection: string }
  const query = request.query as { filter?: string; limit?: string }
  const filter = query.filter ? JSON.parse(query.filter) : {}
  const limit = query.limit ? parseInt(query.limit) : 20

  return db.collection(collection).find(filter, { limit })
})

fastify.get('/api/:collection/:id', async (request, reply) => {
  const { collection, id } = request.params as { collection: string; id: string }
  const entity = await db.collection(collection).get(id)
  if (!entity) {
    reply.code(404)
    return { error: 'Not found' }
  }
  return entity
})

fastify.post('/api/:collection', async (request, reply) => {
  const { collection } = request.params as { collection: string }
  reply.code(201)
  return db.collection(collection).create(request.body as Record<string, unknown>)
})

// Start server
const start = async () => {
  try {
    await fastify.listen({ port: parseInt(process.env.PORT || '3000') })
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
```

---

## Storage Configuration

### FsBackend Options

```typescript
import { FsBackend } from 'parquedb'

const storage = new FsBackend('./data', {
  // Options will be added in future versions
})

const db = new ParqueDB({ storage })
```

### Data Directory Structure

ParqueDB expects this structure (created automatically):

```
./data/
├── {namespace}/
│   └── data.parquet           # Entity data
├── rels/
│   ├── forward/{ns}.parquet   # Outgoing relationships
│   └── reverse/{ns}.parquet   # Incoming relationships
├── indexes/
│   ├── bloom/{ns}.bloom       # Bloom filters
│   └── secondary/{ns}_{field}.idx
└── events/
    └── current.parquet        # Event log
```

### Custom Data Directory

```typescript
// Use absolute path for production
import { resolve } from 'path'

const dataDir = resolve(process.env.DATA_DIR || './data')
const storage = new FsBackend(dataDir)
```

### Memory Backend (Testing)

For testing without disk I/O:

```typescript
import { MemoryBackend } from 'parquedb'

const storage = new MemoryBackend()
const db = new ParqueDB({ storage })

// Data is lost when process exits
```

---

## Production Deployment

### Environment Configuration

Create a `.env` file:

```bash
# Server
PORT=3000
NODE_ENV=production

# ParqueDB
DATA_DIR=/var/lib/parquedb/data
LOG_LEVEL=info

# Optional: S3-compatible storage
# S3_ENDPOINT=https://s3.amazonaws.com
# S3_BUCKET=my-parquedb-bucket
# AWS_ACCESS_KEY_ID=xxx
# AWS_SECRET_ACCESS_KEY=xxx
```

### systemd Service

Create `/etc/systemd/system/parquedb.service`:

```ini
[Unit]
Description=ParqueDB Server
After=network.target

[Service]
Type=simple
User=parquedb
Group=parquedb
WorkingDirectory=/opt/parquedb
ExecStart=/usr/bin/node dist/server.js
Restart=on-failure
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=parquedb

# Environment
Environment=NODE_ENV=production
Environment=PORT=3000
Environment=DATA_DIR=/var/lib/parquedb/data

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/parquedb

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable parquedb
sudo systemctl start parquedb
sudo systemctl status parquedb
```

### PM2 Process Manager

Install PM2:

```bash
npm install -g pm2
```

Create `ecosystem.config.js`:

```javascript
module.exports = {
  apps: [{
    name: 'parquedb',
    script: 'dist/server.js',
    instances: 'max',  // Use all CPU cores
    exec_mode: 'cluster',
    env: {
      NODE_ENV: 'production',
      PORT: 3000,
      DATA_DIR: '/var/lib/parquedb/data'
    },
    // Restart on memory limit
    max_memory_restart: '1G',
    // Logging
    log_file: '/var/log/parquedb/combined.log',
    error_file: '/var/log/parquedb/error.log',
    out_file: '/var/log/parquedb/out.log',
    // Auto-restart
    watch: false,
    autorestart: true,
  }]
}
```

Start with PM2:

```bash
pm2 start ecosystem.config.js
pm2 save
pm2 startup  # Generate startup script
```

### Reverse Proxy (nginx)

Configure nginx as reverse proxy:

```nginx
# /etc/nginx/sites-available/parquedb
upstream parquedb {
    server 127.0.0.1:3000;
    keepalive 64;
}

server {
    listen 80;
    server_name api.example.com;

    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    # SSL certificates (Let's Encrypt)
    ssl_certificate /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    # SSL settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers on;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256;

    # Gzip compression
    gzip on;
    gzip_types application/json;

    location / {
        proxy_pass http://parquedb;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_cache_bypass $http_upgrade;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Health check endpoint (no auth)
    location /health {
        proxy_pass http://parquedb/health;
        access_log off;
    }
}
```

Enable and restart:

```bash
sudo ln -s /etc/nginx/sites-available/parquedb /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

---

## Docker Deployment

### Dockerfile

```dockerfile
# Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci

# Copy source and build
COPY . .
RUN npm run build

# Production stage
FROM node:20-alpine AS production

WORKDIR /app

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S parquedb -u 1001

# Copy built application
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY package*.json ./

# Create data directory
RUN mkdir -p /data && chown -R parquedb:nodejs /data

# Switch to non-root user
USER parquedb

# Environment
ENV NODE_ENV=production
ENV PORT=3000
ENV DATA_DIR=/data

EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

CMD ["node", "dist/server.js"]
```

### docker-compose.yml

```yaml
version: '3.8'

services:
  parquedb:
    build: .
    ports:
      - "3000:3000"
    volumes:
      - parquedb-data:/data
    environment:
      - NODE_ENV=production
      - PORT=3000
      - DATA_DIR=/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:3000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s

volumes:
  parquedb-data:
    driver: local
```

### Build and Run

```bash
# Build image
docker build -t parquedb .

# Run container
docker run -d \
  --name parquedb \
  -p 3000:3000 \
  -v parquedb-data:/data \
  parquedb

# With docker-compose
docker-compose up -d
```

### Kubernetes Deployment

```yaml
# deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: parquedb
  labels:
    app: parquedb
spec:
  replicas: 3
  selector:
    matchLabels:
      app: parquedb
  template:
    metadata:
      labels:
        app: parquedb
    spec:
      containers:
      - name: parquedb
        image: parquedb:latest
        ports:
        - containerPort: 3000
        env:
        - name: NODE_ENV
          value: "production"
        - name: DATA_DIR
          value: "/data"
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "1Gi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 3000
          initialDelaySeconds: 5
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 3000
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: parquedb-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: parquedb
spec:
  selector:
    app: parquedb
  ports:
  - port: 80
    targetPort: 3000
  type: ClusterIP
```

---

## Monitoring

### Health Check Endpoint

```typescript
app.get('/health', async (req, res) => {
  try {
    // Test database connectivity
    await db.Posts.find({}, { limit: 1 })

    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      memory: process.memoryUsage()
    })
  } catch (error) {
    res.status(503).json({
      status: 'unhealthy',
      error: error instanceof Error ? error.message : 'Unknown error'
    })
  }
})
```

### Metrics Endpoint (Prometheus)

```typescript
import { collectDefaultMetrics, Registry, Counter, Histogram } from 'prom-client'

const register = new Registry()
collectDefaultMetrics({ register })

// Custom metrics
const queryDuration = new Histogram({
  name: 'parquedb_query_duration_seconds',
  help: 'Duration of ParqueDB queries',
  labelNames: ['operation', 'collection'],
  buckets: [0.01, 0.05, 0.1, 0.5, 1, 5]
})
register.registerMetric(queryDuration)

const queryTotal = new Counter({
  name: 'parquedb_queries_total',
  help: 'Total number of ParqueDB queries',
  labelNames: ['operation', 'collection', 'status']
})
register.registerMetric(queryTotal)

// Metrics endpoint
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', register.contentType)
  res.end(await register.metrics())
})

// Use in queries
const end = queryDuration.startTimer({ operation: 'find', collection: 'posts' })
try {
  const result = await db.Posts.find(filter)
  queryTotal.inc({ operation: 'find', collection: 'posts', status: 'success' })
  end()
  return result
} catch (error) {
  queryTotal.inc({ operation: 'find', collection: 'posts', status: 'error' })
  end()
  throw error
}
```

---

## Performance Tuning

### Node.js Configuration

```bash
# Increase memory limit
NODE_OPTIONS="--max-old-space-size=4096" node dist/server.js

# Enable garbage collection logging
NODE_OPTIONS="--expose-gc --trace-gc" node dist/server.js
```

### Cluster Mode

```typescript
import cluster from 'cluster'
import os from 'os'

if (cluster.isPrimary) {
  const numCPUs = os.cpus().length
  console.log(`Primary ${process.pid} starting ${numCPUs} workers`)

  for (let i = 0; i < numCPUs; i++) {
    cluster.fork()
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died, restarting...`)
    cluster.fork()
  })
} else {
  // Worker process - start server
  startServer()
}
```

### Query Optimization

```typescript
// Use indexes for frequent queries
await db.Posts.find({ status: 'published' }, {
  // Use secondary index on status
  hint: { index: 'status' }
})

// Project only needed fields
await db.Posts.find({}, {
  project: { title: 1, status: 1, createdAt: 1 }
})

// Use cursor pagination for large datasets
let cursor: string | undefined
do {
  const result = await db.Posts.find({}, { limit: 100, cursor })
  processItems(result.items)
  cursor = result.nextCursor
} while (cursor)
```

---

## Troubleshooting

### "ENOENT: no such file or directory"

**Cause**: Data directory doesn't exist or lacks permissions.

**Solution**:
```bash
mkdir -p /var/lib/parquedb/data
chown -R parquedb:parquedb /var/lib/parquedb
```

### "EMFILE: too many open files"

**Cause**: System file descriptor limit reached.

**Solution**:
```bash
# Temporarily increase limit
ulimit -n 65535

# Permanent: edit /etc/security/limits.conf
parquedb soft nofile 65535
parquedb hard nofile 65535
```

### Memory issues with large queries

**Cause**: Loading too much data into memory.

**Solution**:
```typescript
// Use pagination
const result = await db.Posts.find({}, {
  limit: 100,
  cursor: lastCursor
})

// Use projection
const result = await db.Posts.find({}, {
  project: { $id: 1, title: 1 }  // Only fetch needed fields
})
```

### Slow queries

**Debug**: Enable query logging
```typescript
import { logger } from 'parquedb/utils'
logger.level = 'debug'
```

**Profile**: Use the explain method
```typescript
const plan = await db.Posts.explain({ status: 'published' })
console.log('Query plan:', plan)
```

---

## Next Steps

- [Cloudflare Workers Guide](./cloudflare-workers.md) - Deploy to the edge
- [Configuration Reference](./configuration.md) - All options
- [Architecture Overview](../architecture/) - How ParqueDB works
