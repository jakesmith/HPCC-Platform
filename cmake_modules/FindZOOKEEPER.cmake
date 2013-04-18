################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# - Try to find the ZooKeeper unit testing library
# Once done this will define
#
#  ZOOKEEPER_FOUND - system has the ZooKeeper library
#  ZOOKEEPER_INCLUDE_DIR - the ZooKeeper include directory
#  ZOOKEEPER_LIBRARIES - The libraries needed to use ZooKeeper

IF (NOT ZOOKEEPER_FOUND)
  IF (WIN32)
    SET (zookeeper_dll "zookeeper_mt.dll")
  ELSE()
    SET (zookeeper_dll "zookeeper_mt")
  ENDIF()

  IF (NOT "${EXTERNALS_DIRECTORY}" STREQUAL "")

    IF (UNIX)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "linux64_gcc4.1.1")
      ELSE()
        SET (osdir "linux32_gcc4.1.1")
      ENDIF()
    ELSEIF(WIN32)
      IF (${ARCH64BIT} EQUAL 1)
        SET (osdir "win64")
      ELSE()
        SET (osdir "win32")
      ENDIF()
    ELSE()
      SET (osdir "unknown")
    ENDIF()

    IF (NOT ("${osdir}" STREQUAL "unknown"))
      FIND_PATH (ZOOKEEPER_INCLUDE_DIR NAMES zookeeper/zookeeper.h PATHS "${EXTERNALS_DIRECTORY}/zookeeper/include" NO_DEFAULT_PATH)
      FIND_LIBRARY (ZOOKEEPER_LIBRARIES NAMES ${zookeeper_dll} PATHS "${EXTERNALS_DIRECTORY}/zookeeper/lib/${osdir}" NO_DEFAULT_PATH)
    ENDIF()

  ENDIF()

  # if we didn't find in externals, look in system include path
  if (USE_NATIVE_LIBRARIES)
    FIND_PATH (ZOOKEEPER_INCLUDE_DIR NAMES zookeeper/zookeeper.h)
    FIND_LIBRARY (ZOOKEEPER_LIBRARIES NAMES ${zookeeper_dll})
  endif()

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(ZooKeeper DEFAULT_MSG
    ZOOKEEPER_LIBRARIES
    ZOOKEEPER_INCLUDE_DIR
  )

  IF (ZOOKEEPER_FOUND AND WIN32)
    STRING(REPLACE "zookeeper_dll" "zookeeperd_dll" ZOOKEEPER_DEBUG_LIBRARIES "${ZOOKEEPER_LIBRARIES}")
    set (ZOOKEEPER_LIBRARIES optimized ${ZOOKEEPER_LIBRARIES} debug ${ZOOKEEPER_DEBUG_LIBRARIES})
  ENDIF()
  MARK_AS_ADVANCED(ZOOKEEPER_INCLUDE_DIR ZOOKEEPER_LIBRARIES)
ENDIF()
