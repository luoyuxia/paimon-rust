// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use roaring::RoaringBitmap;
use std::sync::Arc;
use bytes::Buf;

/// DeletionVector represents a set of row positions that have been deleted.
/// Uses RoaringBitmap for efficient storage, similar to Java's BitmapDeletionVector.
///
/// Impl Reference: <https://github.com/apache/paimon/blob/release-0.8.2/paimon-core/src/main/java/org/apache/paimon/deletionvectors/BitmapDeletionVector.java>
#[derive(Debug, Clone)]
pub struct DeletionVector {
    /// RoaringBitmap storing deleted row positions (0-indexed)
    /// Using u32 as RoaringBitmap32 in Java supports up to 2^31-1 rows
    bitmap: Arc<RoaringBitmap>,
}

/// Magic number for BitmapDeletionVector serialization format
/// Same as Java: 1581511376
const MAGIC_NUMBER: u32 = 1581511376;
const MAGIC_NUMBER_SIZE_BYTES: usize = 4;

impl DeletionVector {
    /// Create a new empty DeletionVector
    pub fn empty() -> Self {
        Self {
            bitmap: Arc::new(RoaringBitmap::new()),
        }
    }

    /// Create a new DeletionVector from a RoaringBitmap
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self {
            bitmap: Arc::new(bitmap),
        }
    }

    /// Check if a row at the given position is deleted
    pub fn is_deleted(&self, row_position: u64) -> bool {
        // RoaringBitmap32 in Java supports up to 2^31-1, so we check u32 range
        if row_position > u32::MAX as u64 {
            return false;
        }
        self.bitmap.contains(row_position as u32)
    }

    /// Get the number of deleted rows (cardinality)
    pub fn deleted_count(&self) -> u64 {
        self.bitmap.len()
    }

    /// Check if the deletion vector is empty (no deleted rows)
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Get the underlying bitmap (read-only)
    pub fn bitmap(&self) -> &RoaringBitmap {
        &self.bitmap
    }

    /// Read a DeletionVector from bytes, similar to Java DeletionVector.read(DataInputStream, length)
    /// 
    /// Format (as read by DeletionVector.read):
    /// - bitmapLength (4 bytes int): total size including magic
    /// - magicNumber (4 bytes int): BitmapDeletionVector.MAGIC_NUMBER
    /// - bitmap data (bitmapLength - 4 bytes): serialized RoaringBitmap
    /// - CRC (4 bytes): checksum (skipped during read)
    pub fn read_from_bytes(bytes: &[u8], expected_length: Option<u64>) -> crate::Result<Self> {
        use bytes::Buf;
        
        if bytes.len() < 8 {
            return Err(crate::Error::DataInvalid {
                message: "Deletion vector data too short".to_string(),
                source: None,
            });
        }
        
        let mut buf = bytes;
        
        // Read bitmapLength (total size including magic)
        let bitmap_length = buf.get_i32() as usize;
        
        // Read magic number
        let magic_number = buf.get_i32() as u32;
        if magic_number != MAGIC_NUMBER {
            return Err(crate::Error::DataInvalid {
                message: format!("Invalid magic number: expected {}, got {}", MAGIC_NUMBER, magic_number),
                source: None,
            });
        }
        
        // Verify length if provided
        if let Some(expected) = expected_length {
            if bitmap_length as u64 != expected {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Size not match, actual size: {}, expected size: {}",
                        bitmap_length, expected
                    ),
                    source: None,
                });
            }
        }
        
        // Read bitmap data (bitmapLength - 4 bytes, since magic is already included in bitmapLength)
        let bitmap_data_size = bitmap_length - MAGIC_NUMBER_SIZE_BYTES;
        
        // 4(bitmap_length) + 4(magic_number) + bitmap_data_size + 4(crc)
        if bytes.len() < 8 + bitmap_data_size + 4 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Deletion vector data incomplete: need {} bytes, got {}",
                    8 + bitmap_data_size + 4, bytes.len()
                ),
                source: None,
            });
        }
        
        let bitmap_data = &bytes[8..8 + bitmap_data_size];
        
        // Skip CRC (4 bytes) - Java code does: dis.skipBytes(4)
        // We don't need to verify it here as it's skipped
        
        // Deserialize RoaringBitmap
        let bitmap = RoaringBitmap::deserialize_from(bitmap_data)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to deserialize RoaringBitmap: {}", e),
                source: Some(Box::new(e)),
            })?;
        
        Ok(Self::from_bitmap(bitmap))
    }

    /// Deserialize a DeletionVector from bytes
    /// 
    /// Format (same as Java BitmapDeletionVector.serializeTo):
    /// - Size (4 bytes): total size of data block (magic + bitmap serialized)
    /// - Magic number (4 bytes): 1581511376
    /// - Data: serialized RoaringBitmap
    /// - Checksum (4 bytes): CRC32 checksum of the entire data block (size + magic + bitmap)
    pub fn deserialize_from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        use bytes::Buf;
        use crc32fast::Hasher;
        
        if bytes.len() < 4 + MAGIC_NUMBER_SIZE_BYTES + 4 {
            return Err(crate::Error::DataInvalid {
                message: "Deletion vector data too short".to_string(),
                source: None,
            });
        }

        let mut buf = bytes;
        
        // Read size (total size of data block: magic + bitmap)
        let size = buf.get_u32_le() as usize;
        
        if bytes.len() < 4 + size + 4 {
            return Err(crate::Error::DataInvalid {
                message: format!("Deletion vector data incomplete: expected {} bytes, got {}", 
                    4 + size + 4, bytes.len()),
                source: None,
            });
        }

        // Read magic number
        let magic = buf.get_u32_le();
        if magic != MAGIC_NUMBER {
            return Err(crate::Error::DataInvalid {
                message: format!("Invalid magic number: expected {}, got {}", MAGIC_NUMBER, magic),
                source: None,
            });
        }

        // Read serialized bitmap data (size includes magic, so bitmap data is size - 4)
        let bitmap_data_size = size - MAGIC_NUMBER_SIZE_BYTES;
        let bitmap_data = &bytes[4 + MAGIC_NUMBER_SIZE_BYTES..4 + MAGIC_NUMBER_SIZE_BYTES + bitmap_data_size];
        
        // Read and verify checksum (checksum covers: size + magic + bitmap)
        let expected_checksum = buf.get_u32_le();
        let mut hasher = Hasher::new();
        hasher.update(&bytes[0..4 + size]); // Checksum covers size (4 bytes) + data block (size bytes)
        let actual_checksum = hasher.finalize() as u32;
        
        if expected_checksum != actual_checksum {
            return Err(crate::Error::DataInvalid {
                message: format!("Checksum mismatch: expected {}, got {}", expected_checksum, actual_checksum),
                source: None,
            });
        }

        // Deserialize RoaringBitmap
        let bitmap = RoaringBitmap::deserialize_from(bitmap_data)
            .map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to deserialize RoaringBitmap: {}", e),
                source: Some(Box::new(e)),
            })?;

        Ok(Self::from_bitmap(bitmap))
    }
}

impl Default for DeletionVector {
    fn default() -> Self {
        Self::empty()
    }
}

