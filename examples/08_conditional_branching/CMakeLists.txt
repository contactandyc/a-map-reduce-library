# SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_map_reduce_library_08_conditional_branching LANGUAGES C)

if(CMAKE_PREFIX_PATH)
  include_directories("${CMAKE_PREFIX_PATH}/include")
  link_directories("${CMAKE_PREFIX_PATH}/lib")
endif()

find_library(M_LIB m)

find_package(the_io_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(conditional_branching
  src/main.c
)

target_include_directories(conditional_branching PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(conditional_branching PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
# target_link_libraries(conditional_branching PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(conditional_branching PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(conditional_branching PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(conditional_branching PRIVATE /W4)
else()
  target_compile_options(conditional_branching PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS conditional_branching RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
add_executable(conditional_pipeline_branching
  src/main_pipeline.c
)

target_include_directories(conditional_pipeline_branching PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(conditional_pipeline_branching PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
# target_link_libraries(conditional_pipeline_branching PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(conditional_pipeline_branching PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(conditional_pipeline_branching PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(conditional_pipeline_branching PRIVATE /W4)
else()
  target_compile_options(conditional_pipeline_branching PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS conditional_pipeline_branching RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
