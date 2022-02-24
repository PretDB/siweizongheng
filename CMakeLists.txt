cmake_minimum_required(VERSION 3.10)

project(siweizongheng)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_COMPILER clang++-10)
set(CMAKE_C_COMPILER clang-10)

include(ExternalProject)
ExternalProject_Add(google-benchmark
    GIT_REPOSITORY  https://github.com/google/benchmark.git
    GIT_TAG         7cb2914292886a1c2625faf5cc84743d1855d21b
    CMAKE_ARGS      -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_LIST_DIR}/install;
                    -DCMAKE_BUILD_TYPE=Release;
                    -DBENCHMARK_DOWNLOAD_DEPENDENCIES=OFF;
                    -DBENCHMARK_ENABLE_GTEST_TESTS=off;
                    -DBENCHMARK_ENABLE_TESTING=off
    )


set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED
    COMPONENTS system
    )

find_package(Threads REQUIRED)

set(benchmark_DIR ${CMAKE_CURRENT_LIST_DIR}/install/lib/cmake/benchmark)
find_package(benchmark REQUIRED)

find_package(glog REQUIRED)

FILE(GLOB SRCS src/*)
add_executable(main
    ${SRCS}
    )

add_dependencies(main
    google-benchmark
    )

target_include_directories(main PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/include
    ${CMAKE_CURRENT_LIST_DIR}/install/include
    )

target_link_directories(main PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/install/lib
    ${CMAKE_CURRENT_LIST_DIR}/install/lib64
    ${CMAKE_CURRENT_LIST_DIR}/install/libexec
    )

target_link_libraries(main
    Boost::system
    Threads::Threads
    benchmark::benchmark
    benchmark::benchmark_main
    glog::glog
    )


install(TARGETS main
    CONFIGURATIONS  Debug
    DESTINATION     ${CMAKE_CURRENT_LIST_DIR}/targets/Debug
    )
install(TARGETS main
    CONFIGURATIONS  Release
    DESTINATION     ${CMAKE_CURRENT_LIST_DIR}/targets/Release
    )
