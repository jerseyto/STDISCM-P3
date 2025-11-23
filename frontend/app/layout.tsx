import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Media Upload Service',
  description: 'Producer-Consumer Media Upload Service',
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

