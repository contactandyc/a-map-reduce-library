# SPDX-FileCopyrightText: 2025–2026 Andy Curtis <contactandyc@gmail.com>
# SPDX-License-Identifier: Apache-2.0
#
# Maintainer: Andy Curtis <contactandyc@gmail.com>

cmake_minimum_required(VERSION 3.20)

# A small "apps" project that builds example/demo binaries.
project(a_map_reduce_library_07_buddies LANGUAGES C)

# Optional math lib on POSIX
find_library(M_LIB m)

# Bring in suite-level dependencies derived by the renderer
find_package(a_map_reduce_library CONFIG REQUIRED)
find_package(a_json_library CONFIG REQUIRED)
find_package(a_json_sax_library CONFIG REQUIRED)
find_package(a_memory_library CONFIG REQUIRED)

# ---- Binaries ----
add_executable(buddies  src/pipeline_amazon.c  src/main.c  src/pipeline_reciprocal_nearest_neighbors.c  src/pipeline_substitutes.c  src/pipeline_inv_freq.c)

target_include_directories(buddies PRIVATE  ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Language standards
set_target_properties(buddies PROPERTIES
  C_STANDARD 23
  C_STANDARD_REQUIRED YES
)
if("CXX" IN_LIST CMAKE_PROJECT_LANGUAGES)
  set_target_properties(buddies PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
  )
endif()

# Link the union of deps computed for this app context
target_link_libraries(buddies PRIVATE  a_map_reduce_library::a_map_reduce_library  a_json_library::a_json_library  a_json_sax_library::a_json_sax_library  a_memory_library::a_memory_library)

# Optional libm
if(M_LIB)
  target_link_libraries(buddies PRIVATE ${M_LIB})
endif()

# Warnings
if(MSVC)
  target_compile_options(buddies PRIVATE /W4)
else()
  target_compile_options(buddies PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Install each app binary (handy for packaging examples)
install(TARGETS buddies RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
