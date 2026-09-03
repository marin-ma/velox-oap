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

// Micro-benchmark for GreaterThanBitmap, the per-64-levels comparison behind
// DefLevelsToBitmap / DefRepLevelsToListInfo for every leaf of a nested
// column. Walks a 1M-level array in fixed-size windows, one call per window,
// for the window sizes the reader uses: 64 (normal), 32 and 8 (page and batch
// tails). Run it before and after a change to LevelComparison.cpp to compare.

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <random>
#include <vector>

#include "velox/dwio/parquet/common/LevelComparison.h"

using namespace facebook::velox::parquet;

namespace {

constexpr int64_t kNumLevels = 1 << 20;

// Returns kNumLevels random levels in [0, 3] plus padding for the last window.
const std::vector<int16_t>& randomLevels() {
  static const std::vector<int16_t> levels = [] {
    std::vector<int16_t> result(kNumLevels + 64);
    std::mt19937 generator(1);
    std::uniform_int_distribution<int> distribution(0, 3);
    for (auto& level : result) {
      level = distribution(generator);
    }
    return result;
  }();
  return levels;
}

// Calls GreaterThanBitmap once per window over the whole level array,
// numIterations times.
void run(int64_t windowSize, int numIterations) {
  const auto& levels = randomLevels();
  uint64_t sink{0};
  for (int i = 0; i < numIterations; ++i) {
    for (int64_t offset = 0; offset + windowSize <= kNumLevels;
         offset += windowSize) {
      sink ^= GreaterThanBitmap(levels.data() + offset, windowSize, 1);
    }
  }
  folly::doNotOptimizeAway(sink);
}

} // namespace

BENCHMARK(greaterThanBitmap64, n) {
  run(64, n);
}
BENCHMARK(greaterThanBitmap32, n) {
  run(32, n);
}
BENCHMARK(greaterThanBitmap8, n) {
  run(8, n);
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
