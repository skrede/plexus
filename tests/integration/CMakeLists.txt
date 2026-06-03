# Integration proofs for the vertical slice: the live TCP loopback round-trip and
# the shared-executor test (asio + mdnspp on one io_context) are gated; the
# hot-path allocation audit is a deterministic inproc test and needs no backend.
# Auto-discovered by the tests glob; the `integration.` ctest prefix lets
# `ctest -R integration` select the whole area. The root tests/CMakeLists.txt is
# never edited.

# The hot-path allocation audit is deterministic (inproc Policy) — always built.
add_executable(test_hot_path_alloc test_hot_path_alloc.cpp)
target_link_libraries(test_hot_path_alloc PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
# tests/ is on the include path so the test can pull in support/alloc_counter.h.
target_include_directories(test_hot_path_alloc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_hot_path_alloc TEST_PREFIX "integration.")

# The deterministic inproc req/res roundtrip + concurrent-outstanding + zero-alloc
# rpc-dispatch gate — always built (inproc Policy, no backend needed).
add_executable(test_reqres_roundtrip_inproc test_reqres_roundtrip_inproc.cpp)
target_link_libraries(test_reqres_roundtrip_inproc PRIVATE
    plexus::plexus
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)
target_include_directories(test_reqres_roundtrip_inproc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
catch_discover_tests(test_reqres_roundtrip_inproc TEST_PREFIX "integration.")

# The live TCP round-trip needs the asio backend.
if(PLEXUS_ENABLE_ASIO_BACKEND)
    add_executable(test_asio_roundtrip test_asio_roundtrip.cpp)
    target_link_libraries(test_asio_roundtrip PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_asio_roundtrip TEST_PREFIX "integration.")

    # The asio control-frame demux smoke test: a frame_header-wrapped subscribe
    # pushed through the real asio reassembler + frame_router over TCP loopback
    # demuxes to the subscribe consumer — proving the frame_header.type byte
    # survives the asio receive path (the receive-contract proof, at the point of
    # change). Same asio gate + `integration.` ctest prefix.
    add_executable(test_asio_frame_contract test_asio_frame_contract.cpp)
    target_link_libraries(test_asio_frame_contract PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_asio_frame_contract TEST_PREFIX "integration.")

    # The live TCP loopback req/res roundtrip: a server-side provider and a
    # client-side caller over a real bidirectional asio connection, each on_data
    # wired straight to a frame_router (the settled header-on receive contract).
    # Loops the FULL roundtrip N>=100 in-body; the ctest invocation is re-run
    # >=3 process runs for cross-process reproducibility. Includes the
    # concurrent-outstanding stress (distinct correlation_ids in flight at once).
    add_executable(test_reqres_roundtrip_asio test_reqres_roundtrip_asio.cpp)
    target_link_libraries(test_reqres_roundtrip_asio PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        Catch2::Catch2WithMain)
    catch_discover_tests(test_reqres_roundtrip_asio TEST_PREFIX "integration.")
endif()

# The shared-executor proof needs both the asio backend and the mdnspp adapter.
if(PLEXUS_ENABLE_ASIO_BACKEND AND PLEXUS_ENABLE_MDNSPP_DISCOVERY)
    add_executable(test_shared_executor test_shared_executor.cpp)
    target_link_libraries(test_shared_executor PRIVATE
        plexus::plexus
        plexus::wire
        plexus::asio
        plexus::mdnspp
        Catch2::Catch2WithMain)
    # The mdnspp adapter surface (and the io_context it shares) compiles at C++23.
    set_target_properties(test_shared_executor PROPERTIES
        CXX_STANDARD 23
        CXX_STANDARD_REQUIRED ON
        CXX_EXTENSIONS OFF)
    catch_discover_tests(test_shared_executor TEST_PREFIX "integration.")
endif()
