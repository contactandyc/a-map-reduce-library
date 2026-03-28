# SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A dynamically generated examples project.
project(a_map_reduce_library_03_pipeline_recommender LANGUAGES C)

find_library(M_LIB m)

find_package(the_io_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)

# ---- Targets ----
add_executable(pipeline_recommender
  src/main.c
  src/pipeline_amazon.c
  src/pipeline_enrich.c
  src/pipeline_co_freq.c
)

target_include_directories(pipeline_recommender PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(pipeline_recommender PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
)
if("CXX" IN_LIST CMAKE_PROJECT_LANGUAGES)
  set_target_properties(pipeline_recommender PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
  )
endif()

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
target_link_libraries(pipeline_recommender PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(pipeline_recommender PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(pipeline_recommender PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(pipeline_recommender PRIVATE /W4)
else()
  target_compile_options(pipeline_recommender PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS pipeline_recommender RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
add_executable(pipeline_sax_recommender
  src/main.c
  src/pipeline_amazon_sax.c
  src/pipeline_enrich.c
  src/pipeline_co_freq.c
)

target_include_directories(pipeline_sax_recommender PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(pipeline_sax_recommender PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
)
if("CXX" IN_LIST CMAKE_PROJECT_LANGUAGES)
  set_target_properties(pipeline_sax_recommender PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
  )
endif()

# Ensure examples running in library stacks link the main library!
if(NOT TARGET a_map_reduce_library::a_map_reduce_library)
  find_package(a_map_reduce_library CONFIG REQUIRED)
endif()
target_link_libraries(pipeline_sax_recommender PRIVATE a_map_reduce_library::a_map_reduce_library)

target_link_libraries(pipeline_sax_recommender PRIVATE
  the_io_library::the_io_library
  a_memory_library::a_memory_library
  the_macro_library::the_macro_library
  a_json_library::a_json_library
  a_json_sax_library::a_json_sax_library
  a_map_reduce_library::a_map_reduce_library
)

if(M_LIB)
  target_link_libraries(pipeline_sax_recommender PRIVATE ${M_LIB})
endif()

if(MSVC)
  target_compile_options(pipeline_sax_recommender PRIVATE /W4)
else()
  target_compile_options(pipeline_sax_recommender PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS pipeline_sax_recommender RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
