# Copyright 2018 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(husky-sql)
cmake_minimum_required(VERSION 3.9.1)

set(CMAKE_CXX_STANDARD 14)
SET(CPACK_PACKAGE_VERSION_MAJOR "1")
SET(CPACK_PACKAGE_VERSION_MINOR "0")
SET(CPACK_PACKAGE_VERSION_PATCH "0")
SET(HUSKYSQL_VERSION "${CPACK_PACKAGE_VERSION_MAJOR}.${CPACK_PACKAGE_VERSION_MINOR}.${CPACK_PACKAGE_VERSION_PATCH}")
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# Build type
if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS_DEBUG "-g -DHUSKYSQL_DEBUG_MODE")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DHUSKYSQL_DEBUG_MODE")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -w -DNDEBUG")
endif()

### Third-party
# Submodule
add_subdirectory(husky)
include_directories(husky)
set(EXTERNAL_DEPENDENCIES husky)

find_package(gflags REQUIRED)
message(STATUS "GFLAGS_LIBRARY ${GFLAGS_LIBRARY}")
message(STATUS "GFLAGS_INCLUDE_DIR ${GFLAGS_INCLUDE_DIR}")
list(APPEND EXTERNAL_INCLUDES ${GFLAGS_INCLUDE_DIR})
list(APPEND EXTERNAL_LIBRARY ${GFLAGS_LIBRARY})
list(APPEND EXTERNAL_DEPENDENCIES gflags)

### Directories
# Sources
add_subdirectory(calcite)
add_subdirectory(husky-sql)

# Examples & Tests
add_subdirectory(test)
