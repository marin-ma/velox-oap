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

#include "velox/common/compression/ZstdCompression.h"
#include <zstd_errors.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::common {
namespace {
Status zstdError(const char* prefixMessage, size_t errorCode) {
  return Status::IOError(prefixMessage, ZSTD_getErrorName(errorCode));
}

class ZstdCompressor : public StreamingCompressor {
 public:
  explicit ZstdCompressor(int32_t compressionLevel);

  ~ZstdCompressor() override;

  Status init();

  Expected<CompressResult> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<FlushResult> flush(uint8_t* output, uint64_t outputLength) override;

  Expected<EndResult> end(uint8_t* output, uint64_t outputLength) override;

 private:
  ZSTD_CStream* stream_;
  int32_t compressionLevel_;
};

class ZstdDecompressor : public StreamingDecompressor {
 public:
  ZstdDecompressor();

  ~ZstdDecompressor() override;

  Status init();

  Expected<DecompressResult> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Status reset() override;

  bool isFinished() override;

 private:
  ZSTD_DStream* stream_;
  bool finished_{false};
};

ZstdDecompressor::ZstdDecompressor() : stream_(ZSTD_createDStream()) {}

ZstdDecompressor::~ZstdDecompressor() {
  ZSTD_freeDStream(stream_);
}

Status ZstdDecompressor::init() {
  finished_ = false;
  size_t ret = ZSTD_initDStream(stream_);
  VELOX_RETURN_IF(ZSTD_isError(ret), zstdError("ZSTD init failed: ", ret));
  return Status::OK();
}

Expected<StreamingDecompressor::DecompressResult> ZstdDecompressor::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  ZSTD_inBuffer inBuffer;
  ZSTD_outBuffer outBuffer;

  inBuffer.src = input;
  inBuffer.size = static_cast<size_t>(inputLength);
  inBuffer.pos = 0;
  outBuffer.dst = output;
  outBuffer.size = static_cast<size_t>(outputLength);
  outBuffer.pos = 0;

  auto ret = ZSTD_decompressStream(stream_, &outBuffer, &inBuffer);
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD decompression failed: ", ret));
  finished_ = (ret == 0);
  return DecompressResult{
      static_cast<uint64_t>(inBuffer.pos),
      static_cast<uint64_t>(outBuffer.pos),
      inBuffer.pos == 0 && outBuffer.pos == 0};
}

Status ZstdDecompressor::reset() {
  return init();
}

bool ZstdDecompressor::isFinished() {
  return finished_;
}

ZstdCompressor::ZstdCompressor(int32_t compressionLevel)
    : stream_(ZSTD_createCStream()), compressionLevel_(compressionLevel) {}

ZstdCompressor::~ZstdCompressor() {
  ZSTD_freeCStream(stream_);
}

Status ZstdCompressor::init() {
  auto ret = ZSTD_initCStream(stream_, compressionLevel_);
  VELOX_RETURN_IF(ZSTD_isError(ret), zstdError("ZSTD init failed: ", ret));
  return Status::OK();
}

Expected<StreamingCompressor::CompressResult> ZstdCompressor::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  ZSTD_inBuffer inBuffer;
  ZSTD_outBuffer outBuffer;

  inBuffer.src = input;
  inBuffer.size = static_cast<size_t>(inputLength);
  inBuffer.pos = 0;
  outBuffer.dst = output;
  outBuffer.size = static_cast<size_t>(outputLength);
  outBuffer.pos = 0;

  auto ret = ZSTD_compressStream(stream_, &outBuffer, &inBuffer);
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD compression failed: ", ret));
  return CompressResult{
      static_cast<uint64_t>(inBuffer.pos),
      static_cast<uint64_t>(outBuffer.pos),
      inBuffer.pos == 0};
}

Expected<StreamingCompressor::FlushResult> ZstdCompressor::flush(
    uint8_t* output,
    uint64_t outputLength) {
  ZSTD_outBuffer outBuffer;

  outBuffer.dst = output;
  outBuffer.size = static_cast<size_t>(outputLength);
  outBuffer.pos = 0;

  auto ret = ZSTD_flushStream(stream_, &outBuffer);
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD flush failed: ", ret));
  return FlushResult{static_cast<uint64_t>(outBuffer.pos), ret > 0};
}

Expected<StreamingCompressor::EndResult> ZstdCompressor::end(
    uint8_t* output,
    uint64_t outputLength) {
  ZSTD_outBuffer outBuffer;

  outBuffer.dst = output;
  outBuffer.size = static_cast<size_t>(outputLength);
  outBuffer.pos = 0;

  auto ret = ZSTD_endStream(stream_, &outBuffer);
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD end failed: ", ret));
  return EndResult{static_cast<uint64_t>(outBuffer.pos), ret > 0};
}
} // namespace

ZstdCodec::ZstdCodec(int32_t compressionLevel)
    : compressionLevel_(
          compressionLevel == kUseDefaultCompressionLevel
              ? kZSTDDefaultCompressionLevel
              : compressionLevel) {}

uint64_t ZstdCodec::maxCompressedLength(uint64_t inputLength) {
  return ZSTD_compressBound(static_cast<size_t>(inputLength));
}

Expected<uint64_t> ZstdCodec::compress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  auto ret = ZSTD_compress(
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength),
      compressionLevel_);
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD compression failed: ", ret));
  return static_cast<uint64_t>(ret);
}

Expected<uint64_t> ZstdCodec::decompress(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  if (output == nullptr) {
    // We may pass a NULL 0-byte output buffer but some zstd versions demand
    // a valid pointer: https://github.com/facebook/zstd/issues/1385
    static uint8_t emptyBuffer;
    VELOX_DCHECK_EQ(outputLength, 0);
    output = &emptyBuffer;
  }

  auto ret = ZSTD_decompress(
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength));
  VELOX_RETURN_UNEXPECTED_IF(
      ZSTD_isError(ret), zstdError("ZSTD decompression failed: ", ret));
  return static_cast<uint64_t>(ret);
}

Expected<uint64_t> ZstdCodec::compressFixedLength(
    const uint8_t* input,
    uint64_t inputLength,
    uint8_t* output,
    uint64_t outputLength) {
  auto ret = ZSTD_compress(
      output,
      static_cast<size_t>(outputLength),
      input,
      static_cast<size_t>(inputLength),
      compressionLevel_);
  if (ZSTD_isError(ret)) {
    // It's fine to hit dest size too small.
    if (ZSTD_getErrorCode(ret) == ZSTD_ErrorCode::ZSTD_error_dstSize_tooSmall) {
      return outputLength;
    }
    return folly::makeUnexpected(zstdError("ZSTD compression failed: ", ret));
  }
  return static_cast<uint64_t>(ret);
}

Expected<std::shared_ptr<StreamingCompressor>>
ZstdCodec::makeStreamingCompressor() {
  auto ptr = std::make_shared<ZstdCompressor>(compressionLevel_);
  VELOX_RETURN_UNEXPECTED_NOT_OK(ptr->init());
  return ptr;
}

Expected<std::shared_ptr<StreamingDecompressor>>
ZstdCodec::makeStreamingDecompressor() {
  auto ptr = std::make_shared<ZstdDecompressor>();
  VELOX_RETURN_UNEXPECTED_NOT_OK(ptr->init());
  return ptr;
}

int32_t ZstdCodec::minimumCompressionLevel() const {
  return ZSTD_minCLevel();
}

int32_t ZstdCodec::maximumCompressionLevel() const {
  return ZSTD_maxCLevel();
}

int32_t ZstdCodec::defaultCompressionLevel() const {
  return kZSTDDefaultCompressionLevel;
}

int32_t ZstdCodec::compressionLevel() const {
  return compressionLevel_;
}

CompressionKind ZstdCodec::compressionKind() const {
  return CompressionKind_ZSTD;
}

std::optional<uint64_t> ZstdCodec::getUncompressedLength(
    const uint8_t* input,
    uint64_t inputLength) const {
  // Read decompressed size from frame if available in input.
  auto decompressedSize = ZSTD_getFrameContentSize(input, inputLength);
  if (decompressedSize == ZSTD_CONTENTSIZE_UNKNOWN ||
      decompressedSize == ZSTD_CONTENTSIZE_ERROR) {
    return std::nullopt;
  }
  return decompressedSize;
}

std::unique_ptr<Codec> makeZstdCodec(int32_t compressionLevel) {
  return std::make_unique<ZstdCodec>(compressionLevel);
}
} // namespace facebook::velox::common
