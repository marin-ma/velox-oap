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

#include <zstd.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include "velox/common/compression/Compression.h"

namespace facebook::velox::common {

constexpr int kZSTDDefaultCompressionLevel = 1;

class ZstdCodec : public Codec {
 public:
  explicit ZstdCodec(int32_t compressionLevel);

  uint64_t maxCompressedLength(uint64_t inputLength) override;

  Expected<uint64_t> compress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<uint64_t> decompress(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength) override;

  Expected<uint64_t> compressFixedLength(
      const uint8_t* input,
      uint64_t inputLength,
      uint8_t* output,
      uint64_t outputLength);

  Expected<std::shared_ptr<StreamingCompressor>> makeStreamingCompressor()
      override;

  Expected<std::shared_ptr<StreamingDecompressor>> makeStreamingDecompressor()
      override;

  int32_t minimumCompressionLevel() const override;

  int32_t maximumCompressionLevel() const override;

  int32_t defaultCompressionLevel() const override;

  int32_t compressionLevel() const override;

  CompressionKind compressionKind() const override;

  std::optional<uint64_t> getUncompressedLength(
      const uint8_t* input,
      uint64_t inputLength) const override;

 private:
  int32_t compressionLevel_;
};

std::unique_ptr<Codec> makeZstdCodec(int32_t compressionLevel);

}; // namespace facebook::velox::common
