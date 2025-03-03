cmake_minimum_required(VERSION 3.27)
project(realsim)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Python 3.10 COMPONENTS Interpreter Development REQUIRED)

# if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
#   set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
#   set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
# endif()

# Detect the installed nanobind package and import it into CMake
execute_process(
  COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
  OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE nanobind_ROOT)
find_package(nanobind CONFIG REQUIRED)

nanobind_add_module(jobs jobs/jobs.cpp jobs/jobs.hpp)

find_package(OpenMP REQUIRED)

nanobind_add_module(compengine_utils compengine_utils.cpp jobs/jobs.hpp jobs/jobs.cpp)
if(OpenMP_CXX_FOUND)
    target_link_libraries(compengine_utils PRIVATE OpenMP::OpenMP_CXX)
endif()


# target_compile_options(compengine_utils PUBLIC $<OPENMP_FLAG>)
# target_link_libraries(compengine_utils PUBLIC OpenMP::OpenMP)