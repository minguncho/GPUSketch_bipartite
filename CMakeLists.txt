cmake_minimum_required(VERSION 3.15)
project(GPUSketch_bipartite LANGUAGES CXX)

include (FetchContent)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

# Make the default build type Release. If user or another
# project sets a different value than use that
if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to default -- Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
endif()
message(STATUS "GraphZeppelin Build Type: ${CMAKE_BUILD_TYPE}")

message(STATUS "CMAKE_CXX_FLAGS_RELEASE: ${CMAKE_CXX_FLAGS_RELEASE}")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message(STATUS "Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif(STATUS "${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message("Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
else()
  message(STATUS "${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

# Check if this project is the top directory or build type is Debug
# If so, build executables, otherwise, only build libraries
get_directory_property(not_root PARENT_DIRECTORY)
if (not_root AND "${CMAKE_BUILD_TYPE}" STREQUAL "Release")
  set(BUILD_EXE OFF)
else()
  set(BUILD_EXE ON)
  message (STATUS "GraphZeppelin building executables")
endif()

# Get StreamingUtilities
FetchContent_Declare(
  StreamingUtilities

  GIT_REPOSITORY  https://github.com/GraphStreamingProject/StreamingUtilities.git
  GIT_TAG         main
)

FetchContent_MakeAvailable(StreamingUtilities)

add_executable(main main.cpp)
target_link_libraries(main PRIVATE StreamingUtilities)
target_compile_options(main PUBLIC -fopenmp)
target_link_options(main PUBLIC -fopenmp)
