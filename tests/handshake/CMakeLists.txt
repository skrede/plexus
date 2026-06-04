# The handshake_fsm behavioral oracle: the exhaustive isolation suite for the
# pure, sans-IO session-handshake state machine. Enumerates the full state x event
# matrix, the outbound and inbound paths, the inbound-only bootstrap common case,
# the version-compat matrix (with the exact-protocol gate ordering), the
# identity-collision gate, greater-node_id-wins dedup arbitration plus its symmetry
# and convergence, the install-once completion latch, the torn-down reset, and the
# compile-time pins. The FSM is transport-free, so this links only the header-only
# core and the wire value structs — no inproc backend. A focused zero-alloc gate
# over a steady step runs through support/alloc_counter.h, so tests/ is on the
# include path. The `handshake.` ctest prefix lets `ctest -R handshake` select it.
# Auto-discovered by the tests glob.
add_executable(test_handshake_fsm test_handshake_fsm.cpp)

# tests/ is on the include path so the oracle can pull in support/alloc_counter.h.
target_include_directories(test_handshake_fsm PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(test_handshake_fsm PRIVATE
    plexus::plexus
    plexus::wire
    Catch2::Catch2WithMain)

catch_discover_tests(test_handshake_fsm TEST_PREFIX "handshake.")
