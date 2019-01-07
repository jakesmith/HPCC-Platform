################################################################################
#    Copyright (C) 2012 HPCC Systems®.
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


# Component: unittests
#####################################################
# Description:
# ------------
#    Cmake Input File for unittests
#####################################################

if (USE_CPPUNIT)

ADD_SUBDIRECTORY(configmgr)

project( unitests )

set (    SRCS
         unittests.cpp
         remotetests.cpp
         dalitests.cpp
         jlibtests.cpp
         cryptotests.cpp
         configmgr/ConfigMgrUnitTests.cpp
         configmgr/ConfigMgrTemplateTests.cpp
    )

include_directories (
         .
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/libbase58
         ${HPCC_SOURCE_DIR}/dali/dafsclient
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/configuration/configmgr/configmgrlib
         ${HPCC_SOURCE_DIR}/configuration/configmgr/RapidJSON/include
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( unittests ${SRCS} )

install ( TARGETS unittests RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( unittests
         jlib
         dafsclient
         dalibase
         deftype
         libbase58
         thorhelper
         configmgr
         ${CPPUNIT_LIBRARIES}
    )

endif ()
