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
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::functions::sparksql {
namespace {

class MakeTimestamp : public exec::VectorFunction {
 public:
  MakeTimestamp() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, TIMESTAMP(), result);

    std::optional<int64_t> daySinceEpoch = std::nullopt;
    std::optional<int64_t> microsSinceMidnight = std::nullopt;
    std::optional<int64_t> timeZoneID = std::nullopt;
    int64_t sessionTzID = 0;

    if (args[0]->isConstantEncoding() && args[1]->isConstantEncoding() &&
        args[2]->isConstantEncoding()) {
      auto year = args[0]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
      auto month = args[1]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
      auto day = args[2]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
      daySinceEpoch = util::daysSinceEpochFromDate(year, month, day);
    }
    if (args[3]->isConstantEncoding() && args[4]->isConstantEncoding() &&
        args[5]->isConstantEncoding()) {
      auto hour = args[3]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
      auto minute = args[4]->asUnchecked<ConstantVector<int32_t>>()->valueAt(0);
      // In spark, micros is Decimal(18, 6), thus ShortDecimalType.
      auto micros = args[5]->asUnchecked<ConstantVector<int64_t>>()->valueAt(0);
      microsSinceMidnight = hour * util::kMicrosPerHour +
          minute * util::kMicrosPerMinute + micros;
    }
    // Resolve timezone. If timezone is not specified in argument, use session
    // timezone.
    auto hasTimeZone = args.size() == 7;
    if (hasTimeZone) {
      if (args[7]->isConstantEncoding()) {
        timeZoneID =
            util::getTimeZoneID(args[7]
                                    ->asUnchecked<ConstantVector<StringView>>()
                                    ->valueAt(0)
                                    .str());
      }
    } else {
      const auto& queryConfig = context.execCtx()->queryCtx()->queryConfig();
      const auto sessionTzName = queryConfig.sessionTimezone();
      if (!sessionTzName.empty()) {
        sessionTzID = util::getTimeZoneID(sessionTzName);
      }
    }

    auto* resultFlatVector = result->as<FlatVector<Timestamp>>();
    if (daySinceEpoch.has_value()) {
      if (microsSinceMidnight.has_value()) {
        rows.applyToSelected([&](vector_size_t row) {
          auto timestamp =
              util::fromDatetime(*daySinceEpoch, *microsSinceMidnight);
          if (hasTimeZone) {
            if (timeZoneID.has_value()) {
              timestamp.toGMT(*timeZoneID);
            } else {
              exec::LocalDecodedVector decodedTimeZoneID(
                  context, *args[7], rows);
            }
          } else {
            timestamp.toGMT(sessionTzID);
          }
          resultFlatVector->set(row, timestamp);
        });
      } else {
        exec::DecodedArgs decodedArgs(
            rows, {args[3], args[4], args[5]}, context);
        auto hour = decodedArgs.at(0);
        auto minute = decodedArgs.at(1);
        auto micros = decodedArgs.at(2);
        rows.applyToSelected([&](vector_size_t row) {
          auto localMicros =
              hour->valueAt<int32_t>(row) * util::kMicrosPerHour +
              minute->valueAt<int32_t>(row) * util::kMicrosPerMinute +
              micros->valueAt<int64_t>(row);
          auto timestamp = util::fromDatetime(*daySinceEpoch, localMicros);
          if (timeZone) {
            timestamp.toGMT(*timeZone);
          }
          resultFlatVector->set(row, timestamp);
        });
      }
    } else if (microsSinceMidnight.has_value()) {
      exec::DecodedArgs decodedArgs(rows, {args[0], args[1], args[2]}, context);
      auto year = decodedArgs.at(0);
      auto month = decodedArgs.at(1);
      auto day = decodedArgs.at(2);
      rows.applyToSelected([&](vector_size_t row) {
        auto localDaySinceEpoch = util::daysSinceEpochFromDate(
            year->valueAt<int32_t>(row),
            month->valueAt<int32_t>(row),
            day->valueAt<int32_t>(row));
        auto timestamp =
            util::fromDatetime(localDaySinceEpoch, *microsSinceMidnight);
        if (timeZone) {
          timestamp.toGMT(*timeZone);
        }
        resultFlatVector->set(row, timestamp);
      });
    } else {
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto year = decodedArgs.at(0);
      auto month = decodedArgs.at(1);
      auto day = decodedArgs.at(2);
      auto hour = decodedArgs.at(3);
      auto minute = decodedArgs.at(4);
      auto micros = decodedArgs.at(5);
      rows.applyToSelected([&](vector_size_t row) {
        auto localDaySinceEpoch = util::daysSinceEpochFromDate(
            year->valueAt<int32_t>(row),
            month->valueAt<int32_t>(row),
            day->valueAt<int32_t>(row));
        auto localMicros = hour->valueAt<int32_t>(row) * util::kMicrosPerHour +
            minute->valueAt<int32_t>(row) * util::kMicrosPerMinute +
            micros->valueAt<int64_t>(row);
        auto timestamp = util::fromDatetime(localDaySinceEpoch, localMicros);
        if (timeZone) {
          timestamp.toGMT(*timeZone);
        }
        resultFlatVector->set(row, timestamp);
      });
    }
  }
};
} // namespace

} // namespace facebook::velox::functions::sparksql
