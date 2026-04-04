# SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_map_reduce_library_07_buddies LANGUAGES C)

find_library(M_LIB m)

find_package(the_io_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(buddies
  src/pipeline_amazon.c
  src/main.c
  src/pipeline_reciprocal_nearest_neighbors.c
  src/pipeline_substitutes.c
  src/pipeline_inv_freq.c
)

target_include_directories(buddies PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(buddies PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
target_link_libraries(buddies PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(buddies PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(buddies PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(buddies PRIVATE /W4)
else()
  target_compile_options(buddies PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS buddies RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
