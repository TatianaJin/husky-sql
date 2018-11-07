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

execute_process(COMMAND mvn versions:set -DnewVersion=${HUSKYSQL_VERSION} -DgenerateBackupPoms=false WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(HuskySQL_JARS husky-calcite-${HUSKYSQL_VERSION}.jar)

add_custom_command( OUTPUT ${HuskySQL_JARS} COMMAND mvn -Dbuild.dir=${PROJECT_BINARY_DIR} -DskipTests package WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} COMMENT "Build the java directory" VERBATIM)

add_custom_target(java_build ALL DEPENDS ${HuskySQL_JARS})

add_test( NAME java_test COMMAND mvn -Pcmake -Dbuild.dir=${CMAKE_CURRENT_BINARY_DIR} test WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

install( FILES ${HuskySQL_JARS} DESTINATION share)

# Executables
add_custom_target(QueryPlan ALL mvn exec:java -Dexec.mainClass="husky.sql.calcite.planner.SimpleQueryPlanner" -Dexec.args=\"\'select TRANS_ID,ITEM_ID,PRICE from Parts\'\" WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DEPENDS ${HuskySQL_JARS})
add_custom_target(ExecuteQuery ALL mvn exec:java -Dexec.mainClass="husky.sql.calcite.Execute" -Dexec.args=\"\'select TRANS_ID,ITEM_ID,PRICE,SELLER_NAME,BUYER_NAME from Parts\'\" WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DEPENDS ${HuskySQL_JARS})

# Executables
add_custom_target(HuskyQueryPlan ALL mvn exec:java -Dexec.mainClass="husky.sql.calcite.planner.HuskyQueryPlanner" -Dexec.args=\"\'select TRANS_ID,ITEM_ID,PRICE,SELLER_ID,SELLER_NAME from Parts\'\" WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} DEPENDS ${HuskySQL_JARS})
