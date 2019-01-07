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

# Component: dafscontrol 

#####################################################
# Description:
# ------------
#    Cmake Input File for dafscontrol
#####################################################


project( dafscontrol ) 

set (    SRCS 
         dafscontrol.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/system/hrpc 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/dali/dafsclient 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/common/environment 
    )

HPCC_ADD_EXECUTABLE ( dafscontrol ${SRCS} )
set_target_properties (dafscontrol PROPERTIES COMPILE_FLAGS -D_CONSOLE)
install ( TARGETS dafscontrol RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( dafscontrol  
         jlib
         mp 
         hrpc 
         dafsclient 
         dalibase 
         environment 
    )

