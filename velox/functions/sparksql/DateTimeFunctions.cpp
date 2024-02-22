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

#include "velox/functions/sparksql/DateTimeFunctions.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::sparksql {
namespace {

Timestamp makeTimeStampFromDecodedArgs(
    vector_size_t row,
    DecodedVector* year,
    DecodedVector* month,
    DecodedVector* day,
    DecodedVector* hour,
    DecodedVector* minute,
    DecodedVector* micros) {
  auto totalMicros = micros->valueAt<int64_t>(row);
  auto seconds = totalMicros / util::kMicrosPerSec;
  VELOX_USER_CHECK(
      seconds <= 60,
      "Invalid value for SecondOfMinute (valid values 0 - 59): {}.",
      seconds);
  if (seconds == 60) {
    VELOX_USER_CHECK(
        totalMicros % util::kMicrosPerSec == 0,
        "The fraction of sec must be zero. Valid range is [0, 60].");
  }

  auto daysSinceEpoch = util::daysSinceEpochFromDate(
      year->valueAt<int32_t>(row),
      month->valueAt<int32_t>(row),
      day->valueAt<int32_t>(row));
  auto localMicros = hour->valueAt<int32_t>(row) * util::kMicrosPerHour +
      minute->valueAt<int32_t>(row) * util::kMicrosPerMinute +
      micros->valueAt<int64_t>(row);
  return util::fromDatetime(daysSinceEpoch, localMicros);
}

class MakeTimestampFunction : public exec::VectorFunction {
 public:
  MakeTimestampFunction(int64_t sessionTzID) : sessionTzID_(sessionTzID) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, TIMESTAMP(), result);
    auto* resultFlatVector = result->as<FlatVector<Timestamp>>();

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto year = decodedArgs.at(0);
    auto month = decodedArgs.at(1);
    auto day = decodedArgs.at(2);
    auto hour = decodedArgs.at(3);
    auto minute = decodedArgs.at(4);
    auto micros = decodedArgs.at(5);

    if (args.size() == 7) {
      // If the timezone argument is specified, treat the input timestamp as the
      // time in that timezone.
      if (args[6]->isConstantEncoding()) {
        auto constantTzID =
            util::getTimeZoneID(args[6]
                                    ->asUnchecked<ConstantVector<StringView>>()
                                    ->valueAt(0)
                                    .str());
        rows.applyToSelected([&](vector_size_t row) {
          auto timestamp = makeTimeStampFromDecodedArgs(
              row, year, month, day, hour, minute, micros);
          timestamp.toGMT(constantTzID);
          resultFlatVector->set(row, timestamp);
        });
      } else {
        auto timeZone = decodedArgs.at(6);
        rows.applyToSelected([&](vector_size_t row) {
          auto timestamp = makeTimeStampFromDecodedArgs(
              row, year, month, day, hour, minute, micros);
          auto tzID =
              util::getTimeZoneID(timeZone->valueAt<StringView>(row).str());
          timestamp.toGMT(tzID);
          resultFlatVector->set(row, timestamp);
        });
      }
    } else {
      // Otherwise use session timezone. If session timezone is not specified,
      // use default value 0(UTC timezone).
      rows.applyToSelected([&](vector_size_t row) {
        auto timestamp = makeTimeStampFromDecodedArgs(
            row, year, month, day, hour, minute, micros);
        timestamp.toGMT(sessionTzID_);
        resultFlatVector->set(row, timestamp);
      });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        exec::FunctionSignatureBuilder()
            .integerVariable("precision")
            .integerVariable("scale")
            .returnType("timestamp")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("decimal(precision, scale)")
            .build(),
        exec::FunctionSignatureBuilder()
            .integerVariable("precision")
            .integerVariable("scale")
            .returnType("timestamp")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("integer")
            .argumentType("decimal(precision, scale)")
            .argumentType("varchar")
            .build(),
    };
  }

 private:
  int64_t sessionTzID_;
};

std::shared_ptr<exec::VectorFunction> createMakeTimestampFunction(
    const std::string& /* name */,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  VELOX_USER_CHECK(
      inputArgs[5].type->isShortDecimal(),
      "Seconds must be short decimal type but got {}", inputArgs[5].type->toString());
  auto microsType = inputArgs[5].type->asShortDecimal();
  VELOX_USER_CHECK(
      microsType.scale() == 6,
      "Seconds fraction must have 6 digits for microseconds but got {}",
      microsType.scale());

  const auto sessionTzName = config.sessionTimezone();
  const auto sessionTzID =
      sessionTzName.empty() ? 0 : util::getTimeZoneID(sessionTzName);

  return std::make_shared<MakeTimestampFunction>(sessionTzID);
}
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_make_timestamp,
    MakeTimestampFunction::signatures(),
    createMakeTimestampFunction);

} // namespace facebook::velox::functions::sparksql
