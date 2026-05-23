cmake_minimum_required(VERSION 3.25)
project(ColumnarDB)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- utils library ---

add_library(utils INTERFACE)

target_include_directories(utils INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# --- csv library ---

add_library(csv STATIC
        src/csv/csvreader.cpp
        src/csv/csvwriter.cpp
)

target_include_directories(csv PUBLIC
        src/csv
)

# --- schema library (header-only) ---

add_library(schema INTERFACE)

target_include_directories(schema INTERFACE
        src/schema
)

target_link_libraries(schema INTERFACE
        csv
        utils
)

# --- batch library ---

add_library(batch STATIC
        src/engine/batch/batch.cpp
)

target_include_directories(batch PUBLIC
        src/engine/batch
)

target_link_libraries(batch PUBLIC
        csv
        schema
        utils
)

# --- columnar library ---

add_library(columnar STATIC
        src/engine/columnar/columnar_writer.cpp
        src/engine/columnar/columnar_reader.cpp
)

target_include_directories(columnar PUBLIC
        src/engine/columnar
)

target_link_libraries(columnar PUBLIC
        batch
        schema
        utils
)

# --- exec library ---

add_library(exec STATIC
        src/engine/exec/expr.cpp
        src/engine/exec/hash_aggregate.cpp
)

target_include_directories(exec PUBLIC
        src/engine/exec
)

target_link_libraries(exec PUBLIC
        batch
        columnar
        schema
        utils
)

add_executable(clickbench_run bench/clickbench_run.cpp)
target_link_libraries(clickbench_run PRIVATE
        csv
        schema
        batch
        columnar
        exec
        utils
)

add_executable(ColumnarDB main.cpp)
target_link_libraries(ColumnarDB PRIVATE
        csv
        schema
        utils
        columnar
)

enable_testing()
add_subdirectory(tests)
