add_library(DbaseLib Dbase.cpp)

target_include_directories(DbaseLib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(DbaseLib PUBLIC dbaseParser)