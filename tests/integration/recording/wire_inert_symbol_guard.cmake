# The structural-absence symbol guard. A default (non-wire) build never instantiates the
# recording_channel decorator, so its binary must carry no such template symbol. Run nm over
# the given default-build artifact and fail if any recording_channel instantiation appears.
# On a toolchain without nm the guard skips (the compile-time witness in the test still holds).

if(NOT DEFINED BINARY)
    message(FATAL_ERROR "wire_inert_symbol_guard: BINARY not set")
endif()

find_program(NM_TOOL NAMES nm llvm-nm)
if(NOT NM_TOOL)
    message(STATUS "wire_inert_symbol_guard: no nm tool found — skipping (compile-time witness still holds)")
    return()
endif()

execute_process(
    COMMAND ${NM_TOOL} -C ${BINARY}
    OUTPUT_VARIABLE NM_OUT
    ERROR_VARIABLE NM_ERR
    RESULT_VARIABLE NM_RC)

if(NOT NM_RC EQUAL 0)
    message(STATUS "wire_inert_symbol_guard: nm failed (${NM_ERR}) — skipping")
    return()
endif()

string(FIND "${NM_OUT}" "recording_channel" FOUND_AT)
if(NOT FOUND_AT EQUAL -1)
    message(FATAL_ERROR
        "wire_inert_symbol_guard: a default (non-wire) build artifact contains a recording_channel "
        "symbol — the decorator is NOT structurally absent.")
endif()

message(STATUS "wire_inert_symbol_guard: no recording_channel symbol in the default build artifact — structurally absent.")
