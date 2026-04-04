# SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_map_reduce_library_01_word_count LANGUAGES C)

find_library(M_LIB m)

find_package(the_io_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(word_count
  src/main.c
)

target_include_directories(word_count PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(word_count PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
  CXX_STANDARD 17
  CXX_STANDARD_REQUIRED YES
)

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
target_link_libraries(word_count PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(word_count PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(word_count PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(word_count PRIVATE /W4)
else()
  target_compile_options(word_count PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS word_count RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
