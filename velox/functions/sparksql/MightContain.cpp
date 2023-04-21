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
#include "velox/functions/sparksql/MightContain.h"

#include "velox/common/base/BloomFilter.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql {
namespace {
class BloomFilterMightContainFunction final : public exec::VectorFunction {
 public:
  explicit BloomFilterMightContainFunction(
      BloomFilter<StlAllocator<uint64_t>> bloom)
      : bloom_(bloom) {}

  bool isDefaultNullBehavior() const final {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& resultRef) const final {
    VELOX_CHECK_EQ(args.size(), 2);
    context.ensureWritable(rows, BOOLEAN(), resultRef);
    auto& result = *resultRef->as<FlatVector<bool>>();
    exec::DecodedArgs decodedArgs(rows, args, context);
    auto serialized = decodedArgs.at(0);
    auto value = decodedArgs.at(1);
    if (serialized->isConstantMapping() && serialized->isNullAt(0)) {
      rows.applyToSelected([&](int row) { result.setNull(row, true); });
      return;
    }

    if (serialized->isConstantMapping()) {
      rows.applyToSelected([&](int row) {
        auto contain = bloom_.mayContain(
            folly::hasher<int64_t>()(value->valueAt<int64_t>(row)));
        result.set(row, contain);
      });
      return;
    }

    HashStringAllocator allocator{context.pool()};
    rows.applyToSelected([&](int row) {
      BloomFilter output{StlAllocator<uint64_t>(&allocator)};
      output.merge(serialized->valueAt<StringView>(0).str().c_str());
      auto contain = output.mayContain(
          folly::hasher<int64_t>()(value->valueAt<int64_t>(row)));
      result.set(row, contain);
    });
  }

 private:
  BloomFilter<StlAllocator<uint64_t>> bloom_;
};
} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> mightContainSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("boolean")
              .argumentType("varbinary")
              .argumentType("bigint")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeMightContain(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  BaseVector* serialized = inputArgs[0].constantValue.get();
  VELOX_USER_CHECK(
      serialized != nullptr,
      "{} requires first argument to be a constant of type VARBINARY",
      name,
      inputArgs[0].type->toString());
  HashStringAllocator allocator{memory::getDefaultMemoryPool().get()};
  BloomFilter bloom{StlAllocator<uint64_t>(&allocator)};
  bloom.merge(
      serialized->as<ConstantVector<StringView>>()->valueAt(0).str().c_str());
  static const auto kMightContainFunction =
      std::make_shared<BloomFilterMightContainFunction>(std::move(bloom));
  return kMightContainFunction;
}

} // namespace facebook::velox::functions::sparksql
