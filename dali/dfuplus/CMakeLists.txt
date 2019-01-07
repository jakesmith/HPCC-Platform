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


# Component: dfuplus
#####################################################
# Description:
# ------------
#    Cmake Input File for dfuplus
#####################################################

project( dfuplus )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ../../esp/bindings/bindutil.cpp
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp
         dfuplus.cpp
         main.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/dali//dfu
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/dali/dafsclient
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/common/workunit
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

set_source_files_properties (dfuplus.cpp PROPERTIES COMPILE_FLAGS -DDAFILESRV_LOCAL)

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( dfuplus ${SRCS} )
add_dependencies ( dfuplus espscm )
install ( TARGETS dfuplus RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( dfuplus
         jlib
         remote
         dafsclient
         xmllib
         esphttp
    )


