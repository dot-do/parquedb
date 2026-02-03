/**
 * Express Server for Payload CMS with ParqueDB
 *
 * This is the Node.js server for local development.
 * It uses the filesystem backend to store data locally.
 */

import express from 'express'
import payload from 'payload'
import path from 'path'

const app = express()

const start = async () => {
  // Initialize Payload
  await payload.init({
    secret: process.env.PAYLOAD_SECRET || 'your-secret-key-change-in-production',
    express: app,
    onInit: async () => {
      payload.logger.info(`Payload Admin URL: ${payload.getAdminURL()}`)
    },
  })

  // Serve static files for uploads
  app.use('/uploads', express.static(path.join(process.cwd(), 'uploads')))

  // Start the server
  const port = process.env.PORT || 3000
  app.listen(port, () => {
    console.log(`
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║   Payload CMS with ParqueDB                               ║
║                                                           ║
║   Admin Panel: http://localhost:${port}/admin              ║
║   API:         http://localhost:${port}/api                ║
║                                                           ║
║   Data stored in: ./data                                  ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
    `)
  })
}

start().catch(console.error)
