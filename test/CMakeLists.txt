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

include_directories(${PROJECT_SOURCE_DIR} ${HUSKY_EXTERNAL_INCLUDE} ${EXTERNAL_INCLUDES})

include(CTest)

### Reuse gtest in Husky

enable_testing()

get_directory_property(GTEST_LIBRARY DIRECTORY ${PROJECT_SOURCE_DIR}/husky/test DEFINITION GTEST_LIBRARIES GTEST_LIBRARY)
message(STATUS "Will use GTest used in Husky:")
message(STATUS "  (Library should be)       ${GTEST_LIBRARY}")

### Tests
enable_testing()
## Unit Test
file(GLOB_RECURSE UNITTEST_FILES ${PROJECT_SOURCE_DIR}/*_test.cpp)
add_executable(HSQLUnitTest ${UNITTEST_FILES} unittest_main.cpp)
add_dependencies(HSQLUnitTest gtest)
add_dependencies(HSQLUnitTest glog)
add_dependencies(HSQLUnitTest ${EXTERNAL_DEPENDENCIES})
target_link_libraries(HSQLUnitTest ${GTEST_LIBRARY})
target_link_libraries(HSQLUnitTest ${GMOCK_LIBRARY})
target_link_libraries(HSQLUnitTest ${HUSKY_EXTERNAL_LIB})
target_link_libraries(HSQLUnitTest ${EXTERNAL_LIBRARY})
# Uncomment if any resource is needed:
# file(COPY resources DESTINATION ${PROJECT_BINARY_DIR})
add_test(HSQLUnitTest HSQLUnitTest)
list(APPEND TEST_DEPENDENCIES HSQLUnitTest)

## Functional Test

# TestXXX Template
# add_executable(TestXXX test-xxx.cpp)
# target_link_libraries(TestXXX ${husky})
# target_link_libraries(TestXXX ${HUSKY_EXTERNAL_LIB})

add_custom_target(tests COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS ${TEST_DEPENDENCIES})
