/* THIS FILE WAS GENERATED AUTOMATICALLY BY PAYLOAD. */
/* DO NOT MODIFY IT BECAUSE IT COULD BE REWRITTEN AT ANY TIME. */
import type { Metadata } from 'next'

import config from '@payload-config'
import { RootLayout } from '@payloadcms/next/layouts'
import React from 'react'
import { importMap } from './importMap'

import './custom.scss'

type Args = {
  children: React.ReactNode
}

export const metadata: Metadata = {
  title: 'Payload Admin',
  description: 'Payload CMS Admin Panel',
}

export default function Layout({ children }: Args) {
  return <RootLayout config={config} importMap={importMap}>{children}</RootLayout>
}
