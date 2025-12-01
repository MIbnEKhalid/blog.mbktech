import pkg from "pg";
const { Pool } = pkg;
import dotenv from "dotenv";
// Import additional S3 commands for batch operations
import { S3Client, ListObjectsV2Command, PutObjectCommand, GetObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, HeadObjectCommand } from '@aws-sdk/client-s3';

dotenv.config(); // Load environment variables FIRST

// Helper: parse JSON env variable (safely)
function parseJsonEnv(envVar, fallback = {}) {
    if (!envVar) return fallback;
    try {
        return JSON.parse(envVar);
    } catch (e) {
        console.error(`❌ Error parsing JSON for env: ${envVar.substring(0, 20)}... -> ${e.message}`);
        return fallback;
    }
}

const r2Config = parseJsonEnv(process.env.R2_Bucket, {});

const idleTimeoutMillisV = 60000; // 60 seconds
const connectionTimeoutMillisV = 50000; // 5 seconds

// PostgreSQL connection pool for pool
const poolConfig = {
  connectionString: process.env.NEON_POSTGRES,
  ssl: {
    rejectUnauthorized: true,
  },
  max: 20, // Maximum number of connections
  idleTimeoutMillis: idleTimeoutMillisV,
  connectionTimeoutMillis: connectionTimeoutMillisV,
};

export const pool = new Pool(poolConfig);

// Test connection for pool
(async () => {
  try {
    const client = await pool.connect();
    console.log("Connected to neon PostgreSQL database (pool)!");
    client.release();
  } catch (err) {
    console.error("Database connection error (pool):", err);
  }
})();

export const r2Client = new S3Client({
  region: 'auto',
  endpoint: r2Config.ENDPOINT,
  credentials: {
    accessKeyId: r2Config.ACCESS_KEY_ID,
    secretAccessKey: r2Config.SECRET_ACCESS_KEY,
  },
  // Performance optimizations
  maxAttempts: 3,
  retryMode: 'adaptive',
  requestTimeout: 30000, // 30 seconds
  // Connection pooling for better performance
  requestHandler: {
    connectionTimeout: 5000,
    socketTimeout: 30000,
    maxSockets: 50,
    keepAlive: true,
    keepAliveMsecs: 1000,
  },
  // Enable request compression
  useAccelerateEndpoint: false,
  forcePathStyle: true, // Required for some S3-compatible services
});


const BUCKET_NAME = r2Config.BUCKET_NAME;

// Health check for R2 connection (moved up for startup test)
async function checkR2Health() {
  try {
    const startTime = Date.now();
    
    // Try to list objects with minimal result
    const command = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      MaxKeys: 1,
    });
    
    const result = await r2Client.send(command);
    const responseTime = Date.now() - startTime;
    
    return {
      status: 'healthy',
      responseTime,
      bucket: BUCKET_NAME,
      region: 'auto',
      checkedAt: new Date().toISOString()
    };
  } catch (error) {
    return {
      status: 'unhealthy',
      error: error.message,
      bucket: BUCKET_NAME,
      checkedAt: new Date().toISOString()
    };
  }
}

// Test R2 connection on startup
(async () => {
  try {
    const health = await checkR2Health();
    if (health.status === 'healthy') {
      console.log(`✅ Connected to R2 bucket: ${BUCKET_NAME} (${health.responseTime}ms)`);
    } else {
      console.error(`❌ R2 connection failed:`, health.error);
    }
  } catch (err) {
    console.error("R2 connection test error:", err.message);
  }
})();

// Upload file with enhanced features
export async function uploadFile(key, fileBuffer, contentType, options = {}) {
  try {
    // Validate inputs
    if (!key || !fileBuffer) {
      throw new Error('Key and file buffer are required');
    }

    const {
      metadata = {},
      cacheControl = 'public, max-age=31536000', // 1 year default
      storageClass = 'STANDARD',
      serverSideEncryption = 'AES256'
    } = options;

    // Add default metadata (R2 supports metadata)
    const defaultMetadata = {
      'uploaded-at': new Date().toISOString(),
      'file-size': fileBuffer.length.toString(),
      'upload-source': 'web-portal',
      ...metadata
    };

    const command = new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      Body: fileBuffer,
      ContentType: contentType,
      CacheControl: cacheControl,
      Metadata: defaultMetadata,
      ServerSideEncryption: serverSideEncryption,
      StorageClass: storageClass,
      // Note: R2 doesn't support object tagging, so we remove the Tagging parameter
    });
    
    const result = await r2Client.send(command);
    
    return {
      ...result,
      fileSize: fileBuffer.length,
      key,
      contentType,
      uploadedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Upload failed for key ${key}:`, error);
    throw new Error(`Upload failed: ${error.message}`);
  }
}

// Download file with enhanced features
export async function downloadFile(key, options = {}) {
  try {
    if (!key) {
      throw new Error('Key is required');
    }

    const {
      range = null,
      ifNoneMatch = null,
      ifModifiedSince = null,
      responseCacheControl = null,
      responseContentType = null
    } = options;

    // Log range requests for debugging
    if (range) {
      console.log(`Range request for ${key}: ${range}`);
    }

    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      ...(range && { Range: range }),
      ...(ifNoneMatch && { IfNoneMatch: ifNoneMatch }),
      ...(ifModifiedSince && { IfModifiedSince: ifModifiedSince }),
      ...(responseCacheControl && { ResponseCacheControl: responseCacheControl }),
      ...(responseContentType && { ResponseContentType: responseContentType }),
    });
    
    const startTime = Date.now();
    const result = await r2Client.send(command);
    const downloadTime = Date.now() - startTime;
    
    // Log performance for large files
    if (result.ContentLength > 10 * 1024 * 1024) {
      console.log(`Large file download: ${key}, Size: ${result.ContentLength} bytes, Time: ${downloadTime}ms`);
    }
    
    return {
      ...result,
      key,
      downloadedAt: new Date().toISOString(),
      downloadTime
    };
  } catch (error) {
    console.error(`Download failed for key ${key}:`, error);
    
    // Handle specific S3 errors
    if (error.name === 'NoSuchKey') {
      throw new Error(`File not found: ${key}`);
    } else if (error.name === 'AccessDenied') {
      throw new Error(`Access denied for file: ${key}`);
    }
    
    throw new Error(`Download failed: ${error.message}`);
  }
}

// Delete file with enhanced error handling
export async function deleteFile(key) {
  try {
    if (!key) {
      throw new Error('Key is required');
    }

    const command = new DeleteObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
    });
    
    const result = await r2Client.send(command);
    
    return {
      ...result,
      key,
      deletedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Delete failed for key ${key}:`, error);
    throw new Error(`Delete failed: ${error.message}`);
  }
}

// Batch delete multiple files
export async function deleteFiles(keys) {
  try {
    if (!keys || !Array.isArray(keys) || keys.length === 0) {
      throw new Error('Keys array is required and must not be empty');
    }

    // S3 allows max 1000 objects per delete request
    const maxBatchSize = 1000;
    const results = [];

    for (let i = 0; i < keys.length; i += maxBatchSize) {
      const batch = keys.slice(i, i + maxBatchSize);
      
      const command = new DeleteObjectsCommand({
        Bucket: BUCKET_NAME,
        Delete: {
          Objects: batch.map(key => ({ Key: key })),
          Quiet: false
        }
      });

      const result = await r2Client.send(command);
      results.push(result);
    }

    return {
      results,
      deletedCount: results.reduce((acc, result) => acc + (result.Deleted?.length || 0), 0),
      errors: results.reduce((acc, result) => acc.concat(result.Errors || []), []),
      deletedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error('Batch delete failed:', error);
    throw new Error(`Batch delete failed: ${error.message}`);
  }
}

// List files with enhanced pagination and filtering
export async function listfiles(prefix = '', options = {}) {
  try {
    const {
      maxKeys = 1000,
      continuationToken = null,
      delimiter = null,
      fetchOwner = false,
      startAfter = null
    } = options;

    const command = new ListObjectsV2Command({
      Bucket: BUCKET_NAME,
      Prefix: prefix,
      MaxKeys: maxKeys,
      ...(continuationToken && { ContinuationToken: continuationToken }),
      ...(delimiter && { Delimiter: delimiter }),
      ...(fetchOwner && { FetchOwner: fetchOwner }),
      ...(startAfter && { StartAfter: startAfter }),
    });
    
    const result = await r2Client.send(command);
    
    return {
      ...result,
      requestedAt: new Date().toISOString(),
      totalFiles: result.KeyCount || 0,
      hasMore: result.IsTruncated || false,
      nextToken: result.NextContinuationToken || null
    };
  } catch (error) {
    console.error('List files failed:', error);
    throw new Error(`List files failed: ${error.message}`);
  }
}

// Get file metadata without downloading
export async function getFileMetadata(key) {
  try {
    if (!key) {
      throw new Error('Key is required');
    }

    const command = new HeadObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
    });
    
    const result = await r2Client.send(command);
    
    return {
      ...result,
      key,
      exists: true,
      queriedAt: new Date().toISOString()
    };
  } catch (error) {
    if (error.name === 'NotFound' || error.name === 'NoSuchKey') {
      return {
        key,
        exists: false,
        queriedAt: new Date().toISOString()
      };
    }
    
    console.error(`Get metadata failed for key ${key}:`, error);
    throw new Error(`Get metadata failed: ${error.message}`);
  }
}

// Check if file exists
export async function fileExists(key) {
  try {
    const metadata = await getFileMetadata(key);
    return metadata.exists;
  } catch (error) {
    return false;
  }
}

// Get file size without downloading
export async function getFileSize(key) {
  try {
    const metadata = await getFileMetadata(key);
    return metadata.exists ? metadata.ContentLength : null;
  } catch (error) {
    return null;
  }
}

// Export health check for external use
export { checkR2Health };

// Generate signed URL for temporary access
export async function generateSignedUrl(key, operation = 'getObject', expiresIn = 3600) {
  try {
    const { getSignedUrl } = await import('@aws-sdk/s3-request-presigner');
    
    let command;
    switch (operation) {
      case 'getObject':
        command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: key });
        break;
      case 'putObject':
        command = new PutObjectCommand({ Bucket: BUCKET_NAME, Key: key });
        break;
      default:
        throw new Error(`Unsupported operation: ${operation}`);
    }
    
    const signedUrl = await getSignedUrl(r2Client, command, { expiresIn });
    
    return {
      url: signedUrl,
      key,
      operation,
      expiresIn,
      expiresAt: new Date(Date.now() + expiresIn * 1000).toISOString(),
      generatedAt: new Date().toISOString()
    };
  } catch (error) {
    console.error(`Generate signed URL failed for key ${key}:`, error);
    throw new Error(`Generate signed URL failed: ${error.message}`);
  }
}