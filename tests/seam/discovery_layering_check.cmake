# Structural regression check for the discovery module seam (the one-way
# arrow: core/api never depend on the discovery implementation, and the
# platform-enumeration primitives stay leaf-only). Pure CMake string matching
# so the check runs identically on macOS, Linux, and Windows with no shell
# grep dependency. Invoked via `cmake -P` from tests/seam/CMakeLists.txt;
# PLEXUS_SOURCE_DIR is the source tree to check, passed at configure time so
# this always inspects source, never the build tree.

if(NOT DEFINED PLEXUS_SOURCE_DIR)
    message(FATAL_ERROR "PLEXUS_SOURCE_DIR must be passed to discovery_layering_check.cmake")
endif()

set(_violations "")

function(_assert_file_clean file forbidden_tokens)
    if(NOT EXISTS "${file}")
        return()
    endif()
    file(READ "${file}" _content)
    foreach(_token IN LISTS forbidden_tokens)
        string(FIND "${_content}" "${_token}" _pos)
        if(NOT _pos EQUAL -1)
            list(APPEND _violations "${file}: forbidden token '${_token}' found")
            set(_violations "${_violations}" PARENT_SCOPE)
        endif()
    endforeach()
endfunction()

function(_assert_tree_clean dir forbidden_tokens)
    if(NOT IS_DIRECTORY "${dir}")
        return()
    endif()
    file(GLOB_RECURSE _headers "${dir}/*.h" "${dir}/*.hpp")
    foreach(_header IN LISTS _headers)
        _assert_file_clean("${_header}" "${forbidden_tokens}")
    endforeach()
    set(_violations "${_violations}" PARENT_SCOPE)
endfunction()

# (a) The one-way arrow: core and api link no plexus::discovery.
_assert_file_clean("${PLEXUS_SOURCE_DIR}/lib/plexus-core/CMakeLists.txt" "plexus::discovery")
_assert_file_clean("${PLEXUS_SOURCE_DIR}/lib/plexus-api/CMakeLists.txt" "plexus::discovery")

# (a) plexus-discovery links only plexus::core + plexus::wire — no backend, no
# asio. Matched as target names (plexus::asio), not the bare word "asio",
# since prose comments legitimately reference "the asio convention" without
# creating a link edge.
_assert_file_clean("${PLEXUS_SOURCE_DIR}/lib/plexus-discovery/CMakeLists.txt"
    "plexus::asio;plexus::freertos;plexus::openssl;plexus::mbedtls")

# (b) The api names only the discovery ABC — never the concrete multicast leaf
# or a backend type. Matched as identifiers (multicast_discovery, asio::),
# not the bare word "asio", for the same reason.
_assert_tree_clean("${PLEXUS_SOURCE_DIR}/lib/plexus-api/include"
    "multicast_discovery;asio::;plexus::asio;plexus::freertos")

# (c) Platform-enumeration primitives stay leaf-only: core, discovery, and api
# grep clean of the socket-enumeration and address-family symbols the asio
# backend alone may reach.
foreach(_area core discovery api)
    _assert_tree_clean("${PLEXUS_SOURCE_DIR}/lib/plexus-${_area}/include"
        "getifaddrs;GetAdaptersAddresses;GetAdapters;AF_INET;sin6_")
endforeach()

list(LENGTH _violations _nviol)
if(_nviol GREATER 0)
    set(_report "discovery layering check: ${_nviol} violation(s):")
    foreach(_v IN LISTS _violations)
        string(APPEND _report "\n  - ${_v}")
    endforeach()
    message(FATAL_ERROR "${_report}")
endif()

message(STATUS "discovery layering check: clean")
