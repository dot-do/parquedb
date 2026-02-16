import React from 'react'

export const metadata = {
  title: 'Payload + ParqueDB',
  description: 'Payload CMS using ParqueDB as the database backend',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <main>{children}</main>
      </body>
    </html>
  )
}
