# SPDX-FileCopyrightText: 2025-2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)

# A small "apps" project that builds example/demo binaries.
project(a-map-reduce-library_04_inverted_recommender LANGUAGES C)

# Optional math lib on POSIX
find_library(M_LIB m)

# Bring in suite-level dependencies derived by the renderer
find_package(the_io_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)
find_package(the_macro_library CONFIG REQUIRED)
find_package(a_map_reduce_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)

# ---- Binaries ----
add_executable(inverted_recommender  src/pipeline_amazon.c  src/main.c  src/pipeline_inv_freq.c)

target_include_directories(inverted_recommender PRIVATE  ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Language standards
set_target_properties(inverted_recommender PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
)
if("CXX" IN_LIST CMAKE_PROJECT_LANGUAGES)
  set_target_properties(inverted_recommender PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
  )
endif()

# Link the union of deps computed for this app context
target_link_libraries(inverted_recommender PRIVATE  the_io_library::the_io_library  a_memory_library::a_memory_library  the_macro_library::the_macro_library  a_map_reduce_library::a_map_reduce_library  a_json_library::a_json_library  a_json_sax_library::a_json_sax_library)

# Optional libm
if(M_LIB)
  target_link_libraries(inverted_recommender PRIVATE ${M_LIB})
endif()

# Warnings
if(MSVC)
  target_compile_options(inverted_recommender PRIVATE /W4)
else()
  target_compile_options(inverted_recommender PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Install each app binary (handy for packaging examples)
install(TARGETS inverted_recommender RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
