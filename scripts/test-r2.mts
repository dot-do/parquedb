#!/usr/bin/env npx tsx
/**
 * Simple R2 connectivity test
 */
import 'dotenv/config'
import { S3Client, HeadBucketCommand, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, ListBucketsCommand, CreateBucketCommand } from '@aws-sdk/client-s3';

const R2_ACCESS_KEY_ID = process.env.R2_ACCESS_KEY_ID;
const R2_SECRET_ACCESS_KEY = process.env.R2_SECRET_ACCESS_KEY;
const R2_URL = process.env.R2_URL;
const R2_BUCKET = process.env.R2_BUCKET ?? 'parquedb';

console.log('Testing R2 connection...');
console.log('  Endpoint:', R2_URL);
console.log('  Bucket:', R2_BUCKET);
console.log('  Has credentials:', !!R2_ACCESS_KEY_ID && !!R2_SECRET_ACCESS_KEY);

const client = new S3Client({
  region: 'auto',
  endpoint: R2_URL,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID!,
    secretAccessKey: R2_SECRET_ACCESS_KEY!,
  },
});

async function main() {
  try {
    // List existing buckets first
    const listResponse = await client.send(new ListBucketsCommand({}));
    console.log('  Available buckets:', listResponse.Buckets?.map(b => b.Name).join(', ') || 'none');

    // Test bucket access or create it
    try {
      await client.send(new HeadBucketCommand({ Bucket: R2_BUCKET }));
      console.log('  Bucket access: SUCCESS');
    } catch (e: any) {
      if (e.name === 'NotFound' || e.$metadata?.httpStatusCode === 404) {
        console.log('  Bucket not found, creating...');
        await client.send(new CreateBucketCommand({ Bucket: R2_BUCKET }));
        console.log('  Bucket created: SUCCESS');
      } else {
        throw e;
      }
    }

    // Test write
    const testKey = `test-${Date.now()}.txt`;
    const testData = Buffer.from('Hello R2!');
    await client.send(new PutObjectCommand({
      Bucket: R2_BUCKET,
      Key: testKey,
      Body: testData,
    }));
    console.log('  Write test: SUCCESS');

    // Test read
    const getResponse = await client.send(new GetObjectCommand({
      Bucket: R2_BUCKET,
      Key: testKey,
    }));
    const body = await getResponse.Body!.transformToByteArray();
    if (Buffer.from(body).toString() === 'Hello R2!') {
      console.log('  Read test: SUCCESS');
    } else {
      console.log('  Read test: MISMATCH');
    }

    // Cleanup
    await client.send(new DeleteObjectCommand({
      Bucket: R2_BUCKET,
      Key: testKey,
    }));
    console.log('  Cleanup: SUCCESS');

    console.log('\nR2 connection is working correctly!');
  } catch (e: any) {
    console.error('  Error:', e.message);
    console.error('  Name:', e.name);
    console.error('  Code:', e.Code ?? e.$metadata?.httpStatusCode);
    console.error('  Full:', JSON.stringify(e, null, 2));
    process.exit(1);
  }
}

main();
