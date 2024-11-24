cmake_minimum_required(VERSION 3.10)

project(DataBase)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_subdirectory(lib)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(${PROJECT_NAME} PRIVATE dbaseParser)

target_include_directories(${PROJECT_NAME} PRIVATE lib)