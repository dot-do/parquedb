---
title: Node.js Standalone
description: Deploy ParqueDB as a standalone Node.js server without Cloudflare Workers for development, self-hosted, or on-premises installations.
---

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

## Prerequisites

### System Requirements

- **Node.js**: 18.0.0 or higher (LTS recommended)
- **Memory**: Minimum 512MB RAM, 2GB+ recommended for production
- **Storage**: SSD recommended for optimal performance
- **OS**: Linux, macOS, or Windows

Check your Node.js version:
```bash
node --version  # Should be >= 18.0.0
```

### Installation

```bash
# Using npm
npm install parquedb

# Using pnpm (recommended)
pnpm add parquedb

# Using yarn
yarn add parquedb

# For TypeScript support (recommended)
npm install -D typescript @types/node

# Optional: HTTP framework
npm install express      # Express.js
npm install fastify      # Fastify
npm install hono         # Hono
```

### Verify Installation

```typescript
// test.ts
import { ParqueDB, MemoryBackend } from 'parquedb'

const db = new ParqueDB({
  storage: new MemoryBackend()
})

const result = await db.Posts.create({
  title: 'Hello ParqueDB',
  content: 'Test post'
})

console.log('Installation verified:', result.$id)
```

Run the test:
```bash
npx tsx test.ts
# or
node --loader ts-node/esm test.ts
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

### Hono Example

```typescript
// server.ts
import { Hono } from 'hono'
import { ParqueDB, FsBackend } from 'parquedb'

const app = new Hono()

// Initialize ParqueDB
const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// REST API endpoints
app.get('/api/:collection', async (c) => {
  try {
    const collection = c.req.param('collection')
    const query = c.req.query()
    const filter = query.filter ? JSON.parse(query.filter) : {}
    const limit = query.limit ? parseInt(query.limit) : 20

    const result = await db.collection(collection).find(filter, { limit })
    return c.json(result)
  } catch (error) {
    return c.json({
      error: error instanceof Error ? error.message : 'Query failed'
    }, 500)
  }
})

app.get('/api/:collection/:id', async (c) => {
  try {
    const collection = c.req.param('collection')
    const id = c.req.param('id')
    const entity = await db.collection(collection).get(id)

    if (!entity) {
      return c.json({ error: 'Not found' }, 404)
    }

    return c.json(entity)
  } catch (error) {
    return c.json({
      error: error instanceof Error ? error.message : 'Query failed'
    }, 500)
  }
})

app.post('/api/:collection', async (c) => {
  try {
    const collection = c.req.param('collection')
    const body = await c.req.json()
    const entity = await db.collection(collection).create(body)
    return c.json(entity, 201)
  } catch (error) {
    return c.json({
      error: error instanceof Error ? error.message : 'Create failed'
    }, 500)
  }
})

app.patch('/api/:collection/:id', async (c) => {
  try {
    const collection = c.req.param('collection')
    const id = c.req.param('id')
    const body = await c.req.json()
    const result = await db.collection(collection).update(id, body)
    return c.json(result)
  } catch (error) {
    return c.json({
      error: error instanceof Error ? error.message : 'Update failed'
    }, 500)
  }
})

app.delete('/api/:collection/:id', async (c) => {
  try {
    const collection = c.req.param('collection')
    const id = c.req.param('id')
    const result = await db.collection(collection).delete(id)
    return c.json(result)
  } catch (error) {
    return c.json({
      error: error instanceof Error ? error.message : 'Delete failed'
    }, 500)
  }
})

// Health check
app.get('/health', (c) => {
  return c.json({
    status: 'healthy',
    timestamp: new Date().toISOString()
  })
})

// Start server (Node.js)
export default {
  port: parseInt(process.env.PORT || '3000'),
  fetch: app.fetch,
}

// Or using Node.js adapter
import { serve } from '@hono/node-server'

serve({
  fetch: app.fetch,
  port: parseInt(process.env.PORT || '3000'),
}, (info) => {
  console.log(`ParqueDB server running on http://localhost:${info.port}`)
})
```

---

## Storage Configuration

### FsBackend Options

```typescript
import { FsBackend } from 'parquedb'

// Basic configuration
const storage = new FsBackend('./data')

// With ParqueDB configuration
const db = new ParqueDB({
  storage,
  // Event log configuration
  eventLog: {
    enabled: true,
    maxEvents: 100000,
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
  }
})
```

### Configuration Options

Full ParqueDB configuration reference:

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

const db = new ParqueDB({
  // Storage backend (required)
  storage: new FsBackend('./data'),

  // Event log for time-travel and audit
  eventLog: {
    enabled: true,              // Enable event logging
    maxEvents: 100000,          // Max events before archiving
    maxAge: 30 * 24 * 60 * 60 * 1000, // Max age in ms (30 days)
  },

  // Schema validation
  schema: {
    User: {
      email: 'string!#',        // Required, unique, indexed
      name: 'string',
      role: 'string',
    },
    Post: {
      title: 'string!',
      content: 'text',
      author: '-> User',        // Relationship to User
    }
  }
})
```

### Data Directory Structure

ParqueDB creates this structure automatically on first write:

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

**Directory permissions**:
```bash
# Recommended permissions
chmod 750 /var/lib/parquedb/data
chown -R parquedb:parquedb /var/lib/parquedb/data
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

## Error Handling and Logging

### Error Handling Best Practices

```typescript
import { ParqueDB, FsBackend } from 'parquedb'
import {
  NotFoundError,
  ETagMismatchError,
  AlreadyExistsError,
} from 'parquedb'

const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// Handle specific error types
app.get('/api/:collection/:id', async (req, res) => {
  try {
    const entity = await db.collection(req.params.collection).get(req.params.id)
    if (!entity) {
      return res.status(404).json({ error: 'Not found' })
    }
    res.json(entity)
  } catch (error) {
    if (error instanceof NotFoundError) {
      return res.status(404).json({ error: error.message })
    }
    console.error('Unexpected error:', error)
    res.status(500).json({ error: 'Internal server error' })
  }
})

// Handle version conflicts
app.patch('/api/:collection/:id', async (req, res) => {
  try {
    const result = await db.collection(req.params.collection).update(
      req.params.id,
      req.body,
      { ifMatch: req.headers['if-match'] }
    )
    res.json(result)
  } catch (error) {
    if (error instanceof ETagMismatchError) {
      return res.status(409).json({
        error: 'Version conflict',
        message: error.message
      })
    }
    throw error
  }
})
```

### Logging Configuration

```typescript
import { logger } from 'parquedb'

// Set log level based on environment
logger.level = process.env.NODE_ENV === 'production' ? 'info' : 'debug'

// Custom logger
import winston from 'winston'

const customLogger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
  ],
})

if (process.env.NODE_ENV !== 'production') {
  customLogger.add(new winston.transports.Console({
    format: winston.format.simple(),
  }))
}

// Wrap ParqueDB operations with logging
async function safeQuery(operation: () => Promise<any>) {
  const start = Date.now()
  try {
    const result = await operation()
    customLogger.info({
      operation: 'query',
      duration: Date.now() - start,
      success: true
    })
    return result
  } catch (error) {
    customLogger.error({
      operation: 'query',
      duration: Date.now() - start,
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    })
    throw error
  }
}
```

### Graceful Shutdown

Implement graceful shutdown to ensure data integrity:

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

const db = new ParqueDB({
  storage: new FsBackend('./data')
})

const server = app.listen(PORT)

// Track active requests
let activeRequests = 0
let isShuttingDown = false

// Middleware to track active requests
app.use((req, res, next) => {
  if (isShuttingDown) {
    res.status(503).json({ error: 'Server is shutting down' })
    return
  }
  activeRequests++
  res.on('finish', () => {
    activeRequests--
  })
  next()
})

// Graceful shutdown handler
async function gracefulShutdown(signal: string) {
  console.log(`${signal} received, starting graceful shutdown`)
  isShuttingDown = true

  // Stop accepting new connections
  server.close(() => {
    console.log('HTTP server closed')
  })

  // Wait for active requests to complete (max 30 seconds)
  const shutdownTimeout = setTimeout(() => {
    console.error('Shutdown timeout, forcing exit')
    process.exit(1)
  }, 30000)

  // Poll until all requests complete
  while (activeRequests > 0) {
    console.log(`Waiting for ${activeRequests} active requests...`)
    await new Promise(resolve => setTimeout(resolve, 1000))
  }

  clearTimeout(shutdownTimeout)

  // Flush any pending writes (if applicable)
  // ParqueDB with FsBackend writes are synchronous, but this is good practice
  console.log('All requests completed, exiting')
  process.exit(0)
}

// Listen for shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'))
process.on('SIGINT', () => gracefulShutdown('SIGINT'))

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error)
  gracefulShutdown('uncaughtException')
})

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled rejection at:', promise, 'reason:', reason)
  gracefulShutdown('unhandledRejection')
})
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

## Advanced Topics

### Custom Storage Paths

Organize data by environment or tenant:

```typescript
import { resolve } from 'path'
import { FsBackend } from 'parquedb'

// Environment-specific data directories
const dataDir = resolve(
  process.env.DATA_DIR ||
  (process.env.NODE_ENV === 'production'
    ? '/var/lib/parquedb/data'
    : './data')
)

const db = new ParqueDB({
  storage: new FsBackend(dataDir)
})

// Multi-tenant setup with isolated data directories
class TenantDBManager {
  private dbs = new Map<string, ParqueDB>()
  private baseDir: string

  constructor(baseDir: string) {
    this.baseDir = baseDir
  }

  getDB(tenantId: string): ParqueDB {
    if (!this.dbs.has(tenantId)) {
      const tenantDir = resolve(this.baseDir, tenantId)
      const db = new ParqueDB({
        storage: new FsBackend(tenantDir)
      })
      this.dbs.set(tenantId, db)
    }
    return this.dbs.get(tenantId)!
  }
}

const manager = new TenantDBManager('./tenants')

// Use in request handler
app.use((req, res, next) => {
  const tenantId = req.headers['x-tenant-id'] as string
  if (!tenantId) {
    return res.status(400).json({ error: 'Missing tenant ID' })
  }
  req.db = manager.getDB(tenantId)
  next()
})
```

### Multi-Database Setups

Separate databases for different purposes:

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

// Analytics database (write-heavy)
const analyticsDB = new ParqueDB({
  storage: new FsBackend('./data/analytics')
})

// Application database (balanced read/write)
const appDB = new ParqueDB({
  storage: new FsBackend('./data/app')
})

// Archive database (read-only, historical data)
const archiveDB = new ParqueDB({
  storage: new FsBackend('./data/archive')
})

// Route to appropriate database
app.post('/api/events', async (req, res) => {
  const event = await analyticsDB.Events.create(req.body)
  res.json(event)
})

app.get('/api/users/:id', async (req, res) => {
  const user = await appDB.Users.get(req.params.id)
  res.json(user)
})

app.get('/api/archive/orders', async (req, res) => {
  const { startDate, endDate } = req.query
  const orders = await archiveDB.Orders.find({
    createdAt: { $gte: startDate, $lte: endDate }
  })
  res.json(orders)
})
```

### Backup Strategies

#### Simple File-Based Backup

```bash
#!/bin/bash
# backup.sh

DATA_DIR="/var/lib/parquedb/data"
BACKUP_DIR="/var/backups/parquedb"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Create backup with tar
tar -czf "$BACKUP_DIR/parquedb_$TIMESTAMP.tar.gz" \
  -C "$(dirname $DATA_DIR)" \
  "$(basename $DATA_DIR)"

# Keep only last 7 days of backups
find "$BACKUP_DIR" -name "parquedb_*.tar.gz" -mtime +7 -delete

echo "Backup completed: parquedb_$TIMESTAMP.tar.gz"
```

Add to crontab:
```bash
# Daily backup at 2 AM
0 2 * * * /opt/parquedb/backup.sh
```

#### Application-Level Backup

```typescript
import { ParqueDB, FsBackend } from 'parquedb'
import { createWriteStream } from 'fs'
import { pipeline } from 'stream/promises'
import { createGzip } from 'zlib'
import { exec } from 'child_process'
import { promisify } from 'util'

const execAsync = promisify(exec)

async function backupDatabase() {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const backupPath = `/var/backups/parquedb/backup-${timestamp}.tar.gz`

  console.log(`Creating backup: ${backupPath}`)

  // Use tar to create compressed backup
  await execAsync(
    `tar -czf ${backupPath} -C /var/lib/parquedb data`
  )

  console.log(`Backup completed: ${backupPath}`)

  // Optional: Upload to S3 or other remote storage
  // await uploadToS3(backupPath)

  return backupPath
}

// Scheduled backup endpoint (protect with authentication!)
app.post('/admin/backup', async (req, res) => {
  try {
    const backupPath = await backupDatabase()
    res.json({
      success: true,
      backupPath,
      timestamp: new Date().toISOString()
    })
  } catch (error) {
    console.error('Backup failed:', error)
    res.status(500).json({
      error: error instanceof Error ? error.message : 'Backup failed'
    })
  }
})

// Automatic daily backups
setInterval(async () => {
  try {
    await backupDatabase()
  } catch (error) {
    console.error('Scheduled backup failed:', error)
  }
}, 24 * 60 * 60 * 1000) // 24 hours
```

#### Point-in-Time Recovery

```typescript
import { ParqueDB } from 'parquedb'

// Query historical state using event log
app.get('/api/entities/:id/history', async (req, res) => {
  const { id } = req.params
  const history = await db.getHistory('entities', id)
  res.json(history)
})

// Restore entity to specific point in time
app.post('/api/entities/:id/restore', async (req, res) => {
  const { id } = req.params
  const { timestamp } = req.body

  const entity = await db.getEntityAtTime('entities', id, new Date(timestamp))

  if (!entity) {
    return res.status(404).json({ error: 'Entity not found at timestamp' })
  }

  res.json(entity)
})
```

### Performance Tuning

#### Connection Pooling

For multi-database setups, implement connection pooling:

```typescript
class DBPool {
  private pool: ParqueDB[] = []
  private maxSize: number
  private storage: FsBackend

  constructor(dataDir: string, maxSize = 10) {
    this.maxSize = maxSize
    this.storage = new FsBackend(dataDir)
  }

  async getConnection(): Promise<ParqueDB> {
    if (this.pool.length > 0) {
      return this.pool.pop()!
    }

    if (this.pool.length < this.maxSize) {
      return new ParqueDB({ storage: this.storage })
    }

    // Wait for a connection to become available
    await new Promise(resolve => setTimeout(resolve, 100))
    return this.getConnection()
  }

  releaseConnection(db: ParqueDB): void {
    if (this.pool.length < this.maxSize) {
      this.pool.push(db)
    }
  }
}

const pool = new DBPool('./data')

// Use in request handler
app.use(async (req, res, next) => {
  const db = await pool.getConnection()
  req.db = db
  res.on('finish', () => {
    pool.releaseConnection(db)
  })
  next()
})
```

#### Query Caching

```typescript
import { LRUCache } from 'lru-cache'

// Create cache
const cache = new LRUCache<string, any>({
  max: 500,
  ttl: 1000 * 60 * 5, // 5 minutes
})

// Cache wrapper
async function cachedQuery<T>(
  key: string,
  query: () => Promise<T>
): Promise<T> {
  const cached = cache.get(key)
  if (cached) {
    return cached
  }

  const result = await query()
  cache.set(key, result)
  return result
}

// Use in routes
app.get('/api/posts', async (req, res) => {
  const cacheKey = `posts:${JSON.stringify(req.query)}`
  const posts = await cachedQuery(cacheKey, async () => {
    return db.Posts.find(req.query)
  })
  res.json(posts)
})

// Invalidate cache on mutations
app.post('/api/posts', async (req, res) => {
  const post = await db.Posts.create(req.body)
  // Clear all post-related cache entries
  cache.clear()
  res.json(post)
})
```

#### Bulk Operations

```typescript
// Batch inserts for better performance
app.post('/api/bulk/posts', async (req, res) => {
  const { posts } = req.body

  // Process in batches
  const BATCH_SIZE = 100
  const results = []

  for (let i = 0; i < posts.length; i += BATCH_SIZE) {
    const batch = posts.slice(i, i + BATCH_SIZE)
    const batchResults = await Promise.all(
      batch.map(post => db.Posts.create(post))
    )
    results.push(...batchResults)
  }

  res.json({
    created: results.length,
    items: results
  })
})

// Batch updates
app.patch('/api/bulk/posts', async (req, res) => {
  const { updates } = req.body // [{ id, data }, ...]

  const results = await Promise.all(
    updates.map(({ id, data }) =>
      db.Posts.update(id, data).catch(error => ({
        id,
        error: error.message
      }))
    )
  )

  const succeeded = results.filter(r => !('error' in r))
  const failed = results.filter(r => 'error' in r)

  res.json({
    succeeded: succeeded.length,
    failed: failed.length,
    errors: failed
  })
})
```

#### Read Replicas

For read-heavy workloads, use multiple read-only instances:

```typescript
import { ParqueDB, FsBackend } from 'parquedb'

// Primary (read/write)
const primary = new ParqueDB({
  storage: new FsBackend('/var/lib/parquedb/primary')
})

// Read replicas (read-only)
const replicas = [
  new ParqueDB({ storage: new FsBackend('/var/lib/parquedb/replica1') }),
  new ParqueDB({ storage: new FsBackend('/var/lib/parquedb/replica2') }),
  new ParqueDB({ storage: new FsBackend('/var/lib/parquedb/replica3') }),
]

let replicaIndex = 0

function getReadDB(): ParqueDB {
  // Round-robin load balancing
  const db = replicas[replicaIndex]
  replicaIndex = (replicaIndex + 1) % replicas.length
  return db
}

// Read from replica
app.get('/api/posts', async (req, res) => {
  const db = getReadDB()
  const posts = await db.Posts.find(req.query)
  res.json(posts)
})

// Write to primary
app.post('/api/posts', async (req, res) => {
  const post = await primary.Posts.create(req.body)
  res.json(post)
})

// Replication sync (using rsync or similar)
setInterval(async () => {
  for (let i = 0; i < replicas.length; i++) {
    await execAsync(
      `rsync -av /var/lib/parquedb/primary/ /var/lib/parquedb/replica${i + 1}/`
    )
  }
}, 60000) // Sync every minute
```

---

## Security Best Practices

### File System Permissions

Ensure proper permissions on data directories:

```bash
# Create dedicated user
sudo useradd -r -s /bin/false parquedb

# Set ownership
sudo chown -R parquedb:parquedb /var/lib/parquedb

# Restrict permissions
sudo chmod 750 /var/lib/parquedb/data
sudo chmod 640 /var/lib/parquedb/data/**/*.parquet

# Prevent directory traversal
sudo chattr +i /var/lib/parquedb  # Make immutable (can't delete dir)
```

### Input Validation

Always validate and sanitize user input:

```typescript
import { z } from 'zod'

// Define validation schemas
const CreatePostSchema = z.object({
  title: z.string().min(1).max(200),
  content: z.string().min(1).max(10000),
  status: z.enum(['draft', 'published']),
  tags: z.array(z.string()).max(10).optional(),
})

app.post('/api/posts', async (req, res) => {
  try {
    // Validate input
    const validatedData = CreatePostSchema.parse(req.body)

    // Create post with validated data
    const post = await db.Posts.create(validatedData)
    res.status(201).json(post)
  } catch (error) {
    if (error instanceof z.ZodError) {
      return res.status(400).json({
        error: 'Invalid input',
        details: error.errors
      })
    }
    throw error
  }
})

// Validate collection names to prevent injection
function isValidCollection(name: string): boolean {
  return /^[a-zA-Z][a-zA-Z0-9_]*$/.test(name)
}

app.get('/api/:collection', async (req, res) => {
  const { collection } = req.params

  if (!isValidCollection(collection)) {
    return res.status(400).json({
      error: 'Invalid collection name'
    })
  }

  const result = await db.collection(collection).find(req.query)
  res.json(result)
})
```

### Authentication and Authorization

```typescript
import { ParqueDB, FsBackend } from 'parquedb'
import jwt from 'jsonwebtoken'

const JWT_SECRET = process.env.JWT_SECRET!

// Authentication middleware
async function authenticate(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '')

  if (!token) {
    return res.status(401).json({ error: 'Unauthorized' })
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET)
    req.user = payload
    next()
  } catch (error) {
    return res.status(401).json({ error: 'Invalid token' })
  }
}

// Authorization middleware
function authorize(roles: string[]) {
  return (req, res, next) => {
    if (!req.user || !roles.includes(req.user.role)) {
      return res.status(403).json({ error: 'Forbidden' })
    }
    next()
  }
}

// Protected routes
app.get('/api/admin/users',
  authenticate,
  authorize(['admin']),
  async (req, res) => {
    const users = await db.Users.find({})
    res.json(users)
  }
)

// Row-level security
app.get('/api/posts', authenticate, async (req, res) => {
  // Users can only see their own drafts
  const filter = req.user.role === 'admin'
    ? {}
    : {
        $or: [
          { status: 'published' },
          { authorId: req.user.id, status: 'draft' }
        ]
      }

  const posts = await db.Posts.find(filter)
  res.json(posts)
})
```

### Rate Limiting

Protect against abuse:

```typescript
import rateLimit from 'express-rate-limit'

// General rate limit
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // Limit each IP to 100 requests per window
  message: 'Too many requests from this IP'
})

app.use('/api/', limiter)

// Stricter limit for mutations
const writeLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 50,
  message: 'Too many write requests'
})

app.use('/api/', (req, res, next) => {
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    return writeLimiter(req, res, next)
  }
  next()
})
```

### Encryption at Rest

Encrypt sensitive data before storage:

```typescript
import { createCipheriv, createDecipheriv, randomBytes } from 'crypto'

const ENCRYPTION_KEY = Buffer.from(process.env.ENCRYPTION_KEY!, 'hex')
const ALGORITHM = 'aes-256-gcm'

function encrypt(text: string): string {
  const iv = randomBytes(16)
  const cipher = createCipheriv(ALGORITHM, ENCRYPTION_KEY, iv)
  let encrypted = cipher.update(text, 'utf8', 'hex')
  encrypted += cipher.final('hex')
  const authTag = cipher.getAuthTag()
  return `${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`
}

function decrypt(encryptedData: string): string {
  const [ivHex, authTagHex, encrypted] = encryptedData.split(':')
  const iv = Buffer.from(ivHex, 'hex')
  const authTag = Buffer.from(authTagHex, 'hex')
  const decipher = createDecipheriv(ALGORITHM, ENCRYPTION_KEY, iv)
  decipher.setAuthTag(authTag)
  let decrypted = decipher.update(encrypted, 'hex', 'utf8')
  decrypted += decipher.final('utf8')
  return decrypted
}

// Use in database operations
app.post('/api/secrets', authenticate, async (req, res) => {
  const { secret } = req.body

  // Encrypt before storing
  const encrypted = encrypt(secret)

  const record = await db.Secrets.create({
    userId: req.user.id,
    encryptedData: encrypted,
  })

  res.json({ id: record.$id })
})

app.get('/api/secrets/:id', authenticate, async (req, res) => {
  const record = await db.Secrets.get(req.params.id)

  if (!record || record.userId !== req.user.id) {
    return res.status(404).json({ error: 'Not found' })
  }

  // Decrypt before returning
  const decrypted = decrypt(record.encryptedData)

  res.json({ secret: decrypted })
})
```

### Audit Logging

Track all database operations:

```typescript
// Audit middleware
app.use((req, res, next) => {
  const originalSend = res.send

  res.send = function (data) {
    // Log the operation
    db.AuditLog.create({
      timestamp: new Date().toISOString(),
      userId: req.user?.id || 'anonymous',
      method: req.method,
      path: req.path,
      ip: req.ip,
      userAgent: req.headers['user-agent'],
      statusCode: res.statusCode,
    }).catch(console.error) // Don't fail request if audit logging fails

    return originalSend.call(this, data)
  }

  next()
})

// Query audit logs
app.get('/api/admin/audit-logs', authenticate, authorize(['admin']), async (req, res) => {
  const { startDate, endDate, userId } = req.query

  const logs = await db.AuditLog.find({
    timestamp: { $gte: startDate, $lte: endDate },
    ...(userId && { userId }),
  }, {
    limit: 100,
    sort: { timestamp: -1 }
  })

  res.json(logs)
})
```

### Environment Variables

Never hardcode secrets:

```bash
# .env (DO NOT commit to version control)
NODE_ENV=production
PORT=3000
DATA_DIR=/var/lib/parquedb/data

# Security
JWT_SECRET=your-very-long-random-secret-here
ENCRYPTION_KEY=64-character-hex-string

# Optional: External services
S3_BUCKET=backups
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
```

Load environment variables securely:

```typescript
import { config } from 'dotenv'
import { resolve } from 'path'

// Load .env file
config({ path: resolve(__dirname, '../.env') })

// Validate required environment variables
const requiredEnvVars = [
  'JWT_SECRET',
  'ENCRYPTION_KEY',
  'DATA_DIR',
]

for (const envVar of requiredEnvVars) {
  if (!process.env[envVar]) {
    throw new Error(`Missing required environment variable: ${envVar}`)
  }
}
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

### Performance degradation over time

**Cause**: Parquet file fragmentation or index bloat.

**Solution**: Implement periodic compaction
```typescript
// Manual compaction
await db.compact('posts')

// Scheduled compaction (run during low-traffic periods)
import { scheduleJob } from 'node-schedule'

// Run every Sunday at 3 AM
scheduleJob('0 3 * * 0', async () => {
  console.log('Starting database compaction')
  const collections = ['posts', 'users', 'comments']
  for (const collection of collections) {
    await db.compact(collection)
  }
  console.log('Compaction completed')
})
```

### TypeScript errors with ParqueDB types

**Cause**: Missing or outdated type definitions.

**Solution**:
```bash
# Ensure TypeScript and types are up to date
npm install -D typescript@latest @types/node@latest

# Clear TypeScript cache
rm -rf node_modules/.cache/typescript
```

### Port already in use

**Cause**: Another process is using the port.

**Solution**:
```bash
# Find process using port 3000
lsof -i :3000

# Kill the process
kill -9 <PID>

# Or use a different port
PORT=3001 node dist/server.js
```

### Connection refused or timeout

**Cause**: Firewall blocking connections or service not running.

**Solution**:
```bash
# Check if service is running
systemctl status parquedb

# Check firewall (Ubuntu/Debian)
sudo ufw status
sudo ufw allow 3000/tcp

# Check firewall (CentOS/RHEL)
sudo firewall-cmd --list-all
sudo firewall-cmd --add-port=3000/tcp --permanent
sudo firewall-cmd --reload
```

---

## Migration Guide

### From MongoDB

ParqueDB provides MongoDB-compatible query syntax:

```typescript
// MongoDB
await db.collection('posts').find({ status: 'published' })
await db.collection('posts').findOne({ _id: id })
await db.collection('posts').insertOne(data)
await db.collection('posts').updateOne({ _id: id }, { $set: data })

// ParqueDB (nearly identical)
await db.Posts.find({ status: 'published' })
await db.Posts.get(id)  // Slightly different
await db.Posts.create(data)
await db.Posts.update(id, { $set: data })
```

Key differences:
- Entity IDs use `$id` instead of `_id`
- `findOne` → `get` (takes ID directly)
- `insertOne` → `create`
- Built-in audit fields (`createdAt`, `updatedAt`, `createdBy`, `updatedBy`)

### From SQLite/PostgreSQL

Map SQL concepts to ParqueDB:

```typescript
// SQL: SELECT * FROM posts WHERE status = 'published'
await db.Posts.find({ status: 'published' })

// SQL: SELECT * FROM posts WHERE status = 'published' LIMIT 10
await db.Posts.find({ status: 'published' }, { limit: 10 })

// SQL: SELECT * FROM posts WHERE views > 1000 ORDER BY views DESC
await db.Posts.find(
  { views: { $gt: 1000 } },
  { sort: { views: -1 } }
)

// SQL: JOIN
// Instead of joins, use relationships
await db.Posts.find({ author: userId }, { include: ['author'] })

// Or traverse relationships
const user = await db.Users.get(userId)
const posts = await db.getRelated(user, 'posts')
```

### Data Import

Import existing data from JSON/CSV:

```typescript
import { ParqueDB, FsBackend } from 'parquedb'
import { readFile } from 'fs/promises'

const db = new ParqueDB({
  storage: new FsBackend('./data')
})

// Import from JSON
async function importJSON(file: string, collection: string) {
  const data = JSON.parse(await readFile(file, 'utf-8'))
  const items = Array.isArray(data) ? data : [data]

  console.log(`Importing ${items.length} items...`)

  for (const item of items) {
    await db.collection(collection).create(item)
  }

  console.log('Import completed')
}

// Import from CSV
import { parse } from 'csv-parse/sync'

async function importCSV(file: string, collection: string) {
  const content = await readFile(file, 'utf-8')
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true
  })

  console.log(`Importing ${records.length} records...`)

  for (const record of records) {
    await db.collection(collection).create(record)
  }

  console.log('Import completed')
}

// Usage
await importJSON('./data.json', 'posts')
await importCSV('./users.csv', 'users')
```

---

## Next Steps

- [Cloudflare Workers Guide](./cloudflare-workers.md) - Deploy to the edge
- [Configuration Reference](./configuration.md) - All options
- [Architecture Overview](../architecture/) - How ParqueDB works
