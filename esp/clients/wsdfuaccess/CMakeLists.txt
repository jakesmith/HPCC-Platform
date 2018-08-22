################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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


# Component: wsdfuaccess

#####################################################
# Description:
# ------------
#    Cmake Input File for wsdfuaccess
#####################################################


project( wsdfuaccess ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         wsdfuaccess.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp
    )

include_directories ( 
         ./../../../system/include 
         ./../../../system/xmllib
         ./../../../system/security/shared 
         ./../../../system/security/securesocket 
         ./../../../dali/base
         ./../../../system/mp
         ./../../bindings 
         ./../../bindings/SOAP/client 
         ./../../bindings/SOAP/xpp 
         ./../../../system/jlib 
         ./../../platform 
         ./../../bindings/SOAP/Platform 
    )

ADD_DEFINITIONS( -D_USRDLL -DWSDFUACCESS_EXPORTS )

HPCC_ADD_LIBRARY( wsdfuaccess SHARED ${SRCS} )
install ( TARGETS wsdfuaccess RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( wsdfuaccess 
         jlib
         xmllib 
         esphttp 
         dalibase
    )


