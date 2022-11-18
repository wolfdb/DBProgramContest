cmake_minimum_required (VERSION 3.12)
project (DBProgramContest)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# set(CMAKE_C_COMPILER /usr/local/bin/gcc)
# set(CMAKE_CXX_COMPILER /usr/local/bin/g++)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address -Wall")

include_directories(include)

find_package(Boost 1.70 REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(TBB COMPONENTS tbbmalloc tbbmalloc_proxy tbb_preview)

# remove this when submit
add_subdirectory(fmt)

add_library(database Relation.cpp Operators.cpp Parser.cpp Utils.cpp Joiner.cpp Log.cpp)
target_link_libraries(database fmt::fmt tbb)
target_include_directories(database PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

OPTION(FORCE_TESTS "Build tests, regardless of build type." ON)
if (CMAKE_BUILD_TYPE MATCHES "[Dd][Ee][Bb][Uu][Gg]" OR FORCE_TESTS)
    # add_subdirectory(test)
endif()

add_executable(Driver main.cpp)
target_link_libraries(Driver database)

# Interactive command line tool to translate our query format to SQL queries
add_executable(Query2SQL Query2SQL.cpp)
target_link_libraries(Query2SQL database)

# Test harness
add_executable(harness harness.cpp)

ADD_CUSTOM_TARGET(link_target ALL
  COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/workloads
  ${CMAKE_CURRENT_BINARY_DIR}/workloads)
