################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

# Component: thormaster_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for thormaster_lcr
#####################################################

project( thormaster_lcr ) 

set (    SRCS 
         mawatchdog.cpp 
         thdemonserver.cpp 
         thgraphmanager.cpp 
         thmastermain.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/system/jhtree 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/common/workunit 
         ${HPCC_SOURCE_DIR}/common/environment 
         ${HPCC_SOURCE_DIR}/common/deftype 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/dali/dafsclient 
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/common/dllserver 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/common/thorhelper 
         ${HPCC_SOURCE_DIR}/tools/swapnode
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/rtl/eclrtl 
         ${HPCC_SOURCE_DIR}/thorlcr/thorutil 
         ${HPCC_SOURCE_DIR}/thorlcr/shared 
         ${HPCC_SOURCE_DIR}/thorlcr/graph 
         ${HPCC_SOURCE_DIR}/thorlcr/thorcodectx 
         ${HPCC_SOURCE_DIR}/thorlcr/mfilemanager 
         ${HPCC_SOURCE_DIR}/thorlcr/activities 
         ${HPCC_SOURCE_DIR}/thorlcr/master 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( thormaster_lcr ${SRCS} )
install ( TARGETS thormaster_lcr RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries (  thormaster_lcr 
         jlib
         jhtree 
         dalibase 
         dafsclient
         environment 
         dllserver 
         nbcd 
         eclrtl 
         hql
         deftype 
         workunit 
         graph_lcr 
         thorsort_lcr 
         thorhelper 
         thorcodectx_lcr 
         dalift 
         mfilemanager_lcr 
         graphmaster_lcr 
         activitymasters_lcr 
         swapnodelib
    )



