/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/common/compression/Compression.h"
#include "velox/common/compression/v2/Compression.h"
#include "velox/common/compression/v2/GzipCompression.h"
#include "velox/dwio/common/OutputStream.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/common/compression/CompressionBufferPool.h"
#include "velox/dwio/common/compression/PagedOutputStream.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/dwrf/common/Encryption.h"

namespace facebook::velox::dwrf {

using namespace dwio::common::compression;

constexpr uint8_t PAGE_HEADER_SIZE = 3;

inline std::shared_ptr<facebook::velox::common::CodecOptions>
getDwrfOrcCompressionOptions(
    velox::common::CompressionKind kind,
    int32_t zlibCompressionLevel,
    int32_t zstdCompressionLevel) {
  if (kind == common::CompressionKind_ZLIB ||
      kind == common::CompressionKind_GZIP) {
    auto options =
        std::make_shared<facebook::velox::common::GzipCodecOptions>();
    options->windowBits =
        (-dwio::common::compression::Compressor::DWRF_ORC_ZLIB_WINDOW_BITS);
    options->format = common::GzipFormat::kDeflate;
    options->compressionLevel = zlibCompressionLevel;
    return options;
  }
  if (kind == common::CompressionKind_ZSTD) {
    return std::make_shared<facebook::velox::common::CodecOptions>(
        zstdCompressionLevel);
  }
  return std::make_shared<facebook::velox::common::CodecOptions>();
}

/**
 * Create a compressor for the given compression kind.
 * @param kind The compression type to implement
 * @param bufferPool Pool for compression buffer
 * @param bufferHolder Buffer holder that handles buffer allocation and
 * collection
 * @param config The compression options to use
 */
inline std::unique_ptr<dwio::common::BufferedOutputStream> createCompressor(
    common::CompressionKind kind,
    CompressionBufferPool& bufferPool,
    dwio::common::DataBufferHolder& bufferHolder,
    const Config& config,
    const dwio::common::encryption::Encrypter* encrypter = nullptr) {
  auto options = getDwrfOrcCompressionOptions(
      kind,
      config.get(Config::ZLIB_COMPRESSION_LEVEL),
      config.get(Config::ZSTD_COMPRESSION_LEVEL));
  auto codec = facebook::velox::common::Codec::create(kind, *options);
  if (!codec) {
    if (!encrypter && kind == common::CompressionKind::CompressionKind_NONE) {
      return std::make_unique<dwio::common::BufferedOutputStream>(bufferHolder);
    }
  }
  return std::make_unique<PagedOutputStream>(
      bufferPool,
      bufferHolder,
      config.get(Config::COMPRESSION_THRESHOLD),
      PAGE_HEADER_SIZE,
      std::move(codec),
      encrypter);
}

inline std::shared_ptr<facebook::velox::common::CodecOptions>
getDwrfOrcDecompressionOptions(common::CompressionKind kind) {
  if (kind == common::CompressionKind_ZLIB ||
      kind == common::CompressionKind_GZIP) {
    auto options =
        std::make_shared<facebook::velox::common::GzipCodecOptions>();
    options->windowBits =
        (-dwio::common::compression::Compressor::DWRF_ORC_ZLIB_WINDOW_BITS);
    options->format = common::GzipFormat::kDeflate;
    return options;
  }
  return std::make_shared<facebook::velox::common::CodecOptions>();
}

/**
 * Create a decompressor for the given compression kind.
 * @param kind The compression type to implement
 * @param input The input stream that is the underlying source
 * @param bufferSize The maximum size of the buffer
 * @param pool The memory pool
 */
inline std::unique_ptr<dwio::common::SeekableInputStream> createDecompressor(
    facebook::velox::common::CompressionKind kind,
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    uint64_t bufferSize,
    memory::MemoryPool& pool,
    const std::string& streamDebugInfo,
    const dwio::common::encryption::Decrypter* decryptr = nullptr) {
  const auto& options = getDwrfOrcDecompressionOptions(kind);
  return dwio::common::compression::createDecompressor(
      kind,
      std::move(input),
      bufferSize,
      pool,
      options,
      streamDebugInfo,
      decryptr);
}

} // namespace facebook::velox::dwrf
