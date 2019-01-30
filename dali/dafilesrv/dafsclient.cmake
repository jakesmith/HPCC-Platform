################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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


# Component: dafsclient 
#####################################################
# Description:
# ------------
#    Cmake Input File for dafsclient
#####################################################

project( dafsclient ) 

set (    SRCS 
         dafsclient.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/dali/base 
    )

ADD_DEFINITIONS( -D_USRDLL -DREMOTE_EXPORTS )

HPCC_ADD_LIBRARY( dafsclient SHARED ${SRCS}  )
install ( TARGETS dafsclient RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( dafsclient 
    eclrtl
    jlib
    dalibase
    ${CPPUNIT_LIBRARIES}
    )

IF (USE_OPENSSL)
    target_link_libraries ( dafsclient 
    	securesocket
    )
ENDIF()
