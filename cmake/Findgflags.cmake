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

include (GNUInstallDirs)

if(GFLAGS_SEARCH_PATH)
    find_path(GFLAGS_INCLUDE_DIR NAMES gflags/gflags.h PATHS ${GFLAGS_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    find_library(GFLAGS_LIBRARY NAMES gflags PATHS ${GFLAGS_SEARCH_PATH} NO_SYSTEM_ENVIRONMENT_PATH)
    message(STATUS "Found GFlags in search path ${GFLAGS_SEARCH_PATH}")
    message(STATUS "  (Headers)       ${GFLAGS_INCLUDE_DIR}")
    message(STATUS "  (Library)       ${GFLAGS_LIBRARY}")
else(GFLAGS_SEARCH_PATH)
    include(ExternalProject)
    set(THIRDPARTY_DIR ${PROJECT_BINARY_DIR}/third_party)
    ExternalProject_Add(
        gflags
        GIT_REPOSITORY "https://github.com/gflags/gflags"
        GIT_TAG v2.2.1
        PREFIX ${THIRDPARTY_DIR}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
        CMAKE_ARGS -DWITH_GFLAGS=OFF
        CMAKE_ARGS -DBUILD_TESTING=OFF
        CMAKE_ARGS -DBUILD_SHARED_LIBS=ON
        UPDATE_COMMAND ""
    )
    list(APPEND EXTERNAL_DEPENDENCIES gflags)
    set(GFLAGS_INCLUDE_DIR "${PROJECT_BINARY_DIR}/include")
    set(GFLAGS_LIBRARY "${PROJECT_BINARY_DIR}/lib/${CMAKE_SHARED_LIBRARY_PREFIX}gflags${CMAKE_SHARED_LIBRARY_SUFFIX}")
    message(STATUS "GFlags will be built as a third party")
    message(STATUS "  (Headers should be)       ${GFLAGS_INCLUDE_DIR}")
    message(STATUS "  (Library should be)       ${GFLAGS_LIBRARY}")
endif(GFLAGS_SEARCH_PATH)
