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

# Component: graph_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for graph_lcr
#####################################################

project( graph_lcr ) 

set (    SRCS 
         ../thorutil/thbuf.cpp 
         ../thorutil/thcompressutil.cpp 
         ../thorutil/thmem.cpp 
         ../thorutil/thorport.cpp
         ../thorutil/thalloc.cpp 
         ../thorutil/thormisc.cpp 
         thgraph.cpp 

         ../thorutil/thorport.hpp
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/thorlcr/thorutil 
         ${HPCC_SOURCE_DIR}/system/jhtree 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/common/workunit 
         ${HPCC_SOURCE_DIR}/thorlcr/shared 
         ${HPCC_SOURCE_DIR}/common/deftype 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/dali/dafsclient 
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/common/dllserver 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/thorlcr/thorcodectx 
         ${HPCC_SOURCE_DIR}/thorlcr/mfilemanager 
         ${HPCC_SOURCE_DIR}/rtl/eclrtl 
         ${HPCC_SOURCE_DIR}/common/thorhelper 
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

HPCC_ADD_LIBRARY( graph_lcr SHARED ${SRCS} )
set_target_properties(graph_lcr PROPERTIES 
    COMPILE_FLAGS -D_USRDLL
    DEFINE_SYMBOL GRAPH_EXPORTS )
install ( TARGETS graph_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( graph_lcr 
         jlib
         jhtree 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         thorhelper
         roxiemem
    )

if (USE_TBBMALLOC)
   add_dependencies ( graph_lcr tbb)
   target_link_libraries ( graph_lcr libtbbmalloc_proxy libtbbmalloc)
endif()


