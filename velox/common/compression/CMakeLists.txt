# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

set(VELOX_COMMON_COMPRESSION_SRCS Compression.cpp LzoDecompressor.cpp)
set(VELOX_COMMON_COMPRESSION_LINK_LIBS velox_status Folly::folly)

if(VELOX_ENABLE_COMPRESSION_LZ4)
  list(APPEND VELOX_COMMON_COMPRESSION_SRCS Lz4Compression.cpp
       HadoopCompressionFormat.cpp)
  list(APPEND VELOX_COMMON_COMPRESSION_LINK_LIBS lz4::lz4)
endif()

velox_add_library(velox_common_compression ${VELOX_COMMON_COMPRESSION_SRCS})
velox_link_libraries(
  velox_common_compression
  PUBLIC ${VELOX_COMMON_COMPRESSION_LINK_LIBS}
  PRIVATE velox_exception)
