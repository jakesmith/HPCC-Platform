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


# Component: ws_workunits
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_workunits
#####################################################

project( ws_workunits )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ../../../dali/sasha/sacmd.cpp
         ../../../dali/dfu/dfuutil.cpp
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp
         ${HPCC_SOURCE_DIR}/esp/scm/ws_workunits.ecm
         ${HPCC_SOURCE_DIR}/esp/clients/roxiecontrol.cpp
         ws_workunitsPlugin.cpp
         ws_workunitsService.cpp
         ws_workunitsService.hpp
         ws_workunitsHelpers.cpp
         ws_workunitsHelpers.hpp
         ws_workunitsAuditLogs.cpp
         ws_workunitsQuerySets.cpp
         ws_wudetails.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/esp/esplib
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/dali/dfu
         ${HPCC_SOURCE_DIR}/dali/sasha
         ${HPCC_SOURCE_DIR}/dali/dafsclient
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/roxie/roxie
         ${HPCC_SOURCE_DIR}/esp/services
         ${HPCC_SOURCE_DIR}/esp/common
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/common/wuwebview
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/common/fileview2
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/security/zcrypt
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/http/client
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/dali/dfu
         ${HPCC_SOURCE_DIR}/testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_WORKUNITS_EXPORTS )

HPCC_ADD_LIBRARY( ws_workunits SHARED ${SRCS} )
add_dependencies ( ws_workunits espscm )
install ( TARGETS ws_workunits RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_workunits
         jlib
         mp
         hrpc
         dafsclient
         xmllib
         esphttp
         dalibase
         environment
         dalift
         dllserver
         nbcd
         eclrtl
         deftype
         workunit
         dfuwu
         wuwebview
         SMCLib
         schedulectrl
         roxiecommlib
         hql
         jhtree
         fileview2
         thorhelper
         ${CPPUNIT_LIBRARIES}
    )

IF (USE_OPENSSL)
    target_link_libraries ( ws_workunits
    	securesocket
    )
ENDIF()
IF (USE_ZLIB)
  target_link_libraries ( ws_workunits
      ${ZLIB_LIBRARIES}
      zcrypt
      )
ENDIF()
