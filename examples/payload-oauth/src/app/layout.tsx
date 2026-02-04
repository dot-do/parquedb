import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Payload + ParqueDB + OAuth',
  description: 'Payload CMS using ParqueDB with oauth.do authentication',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}
