include(FetchContent)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(csv_tests
        test_csv.cpp
)

target_link_libraries(csv_tests PRIVATE
        csv
        schema
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(csv_tests)

# ---- ДОБАВИТЬ ЭТО ----
add_executable(columnar_tests
        test_roundtrip.cpp
)

target_link_libraries(columnar_tests PRIVATE
        csv
        schema
        batch
        columnar
        utils
        GTest::gtest_main
)

gtest_discover_tests(columnar_tests)
