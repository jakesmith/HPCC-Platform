message ( STATUS "CC=$ENV{CC}" )

#project ( nothing LANGUAGES C )
project(hpccsystems-platform)

cmake_minimum_required(VERSION 3.13.0)


# Workaround
# unset ( ENV{CC} )
# unset ( ENV{CXX} )

message ( STATUS "CC=$ENV{CC}" )
message ( STATUS "CMAKE_C_COMPILER_VERSION=${CMAKE_C_COMPILER_VERSION}" )
message ( STATUS "CMAKE_COMPILER_IS_GNUCXX=${CMAKE_COMPILER_IS_GNUCXX}" )
message ( STATUS "CMAKE_CXX_COMPILER_VERSION=${CMAKE_CXX_COMPILER_VERSION}" )

add_executable(atest atest.cpp)

set_target_properties (atest PROPERTIES COMPILE_FLAGS -D_CONSOLE)
