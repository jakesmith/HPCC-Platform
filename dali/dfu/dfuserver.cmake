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

# Component: dfuserver 

#####################################################
# Description:
# ------------
#    Cmake Input File for dfuserver
#####################################################


project( dfuserver ) 

set (    SRCS 
         dfurun.cpp 
         dfurunkdp.cpp 
         dfuserver.cpp 
         dfurepl.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/jhtree 
         ${HPCC_SOURCE_DIR}/rtl/eclrtl 
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl 
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/dali/dafsclient
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/dali/ft 
         ${HPCC_SOURCE_DIR}/common/environment 
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

HPCC_ADD_EXECUTABLE ( dfuserver ${SRCS} )
set_target_properties ( dfuserver PROPERTIES 
        COMPILE_FLAGS "-D_CONSOLE -D_DFUSERVER"
        )
install ( TARGETS dfuserver RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( dfuserver 
         jlib
         mp 
         hrpc 
         dalibase 
         dafsclient 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         schedulectrl 
         dalift 
         jhtree 
         dfuwu 
    )
