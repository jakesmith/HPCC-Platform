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


# Component: dfuxref 
#####################################################
# Description:
# ------------
#    Cmake Input File for dfuxref
#####################################################

project( dfuxref ) 

set (    SRCS 
         dfuxrefmain.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/common/remote 
         ${HPCC_SOURCE_DIR}rver 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/dali/ft 
         ${HPCC_SOURCE_DIR}/common/environment 
         ${HPCC_SOURCE_DIR}/dali/dfuXRefLib
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/dafsclient
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

ADD_DEFINITIONS ( -D_CONSOLE -DMAIN_DFUXREF )

HPCC_ADD_EXECUTABLE ( dfuxref ${SRCS} )
install ( TARGETS dfuxref RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( dfuxref 
         jlib
         mp 
         hrpc 
         dafsclient
         dalibase 
         environment 
         dfuXRefLib 
    )


