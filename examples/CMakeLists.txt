# Runnable examples — the public face of the API. Each example is one terminal
# binary. The asio + mDNS examples are guarded on the backend targets so the tree
# builds with whatever backends are enabled; the in-process example needs only the
# header-only inproc backend and builds unconditionally.

function(plexus_example name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE ${ARGN})
endfunction()

if(TARGET plexus::asio AND TARGET plexus::mdnspp)
    plexus_example(bytes_pubsub_pub   plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(bytes_pubsub_sub   plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(bytes_reqres_server plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(bytes_reqres_client plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_pubsub_pub   plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_pubsub_sub   plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_reqres_server plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_reqres_client plexus::plexus plexus::asio plexus::mdnspp)
endif()

# The size-envelope example needs only the asio backend (static discovery is in the
# core), so it is guarded on asio alone rather than the asio+mDNS pair above.
if(TARGET plexus::asio)
    plexus_example(size_envelope_qos plexus::plexus plexus::asio)
endif()

plexus_example(typed_inproc_fastpath plexus::plexus plexus::inproc)

# The logging example projects a typed topic to CSV on stdout via node.log<Codec, Projection>.
plexus_example(logging_example plexus::plexus plexus::inproc)

# The recording example captures a session to a flat stream over the public byte_sink seam.
# Its capture leg builds + runs unconditionally; the optional flat->MCAP step links the
# transcode lib and compiles its branch in only when PLEXUS_BUILD_MCAP_TRANSCODE is on.
plexus_example(recording_example plexus::plexus plexus::inproc)
if(TARGET mcap_transcode_lib)
    target_link_libraries(recording_example PRIVATE mcap_transcode_lib)
    target_compile_definitions(recording_example PRIVATE PLEXUS_WITH_MCAP_TRANSCODE)
endif()

# The shared-memory local node example: a live node composing shm + AF_UNIX + plain TCP as
# variadic leaves. It is Linux-only (the shm bridge headers live under shm/linux/) and needs
# the shm backend target (plexus::asio::shm, present only when PLEXUS_ENABLE_SHM_BACKEND is
# on); plexus::asio::shm carries liburing as a usage requirement, so the example links no
# liburing of its own. Crypto-free: the example pulls in no TLS/DTLS/UDP, so it builds with
# the crypto backend disabled.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND TARGET plexus::asio::shm)
    plexus_example(shm_local_node plexus::plexus plexus::asio plexus::asio::shm plexus::shm)

    # The portable same-host node: one consumer surface, no platform conditional. On Linux
    # the header resolves to the shm + AF_UNIX + TCP set, so it needs the same shm bridge
    # deps as shm_local_node; elsewhere it would build against AF_UNIX + TCP alone.
    plexus_example(same_host_node plexus::plexus plexus::asio plexus::asio::shm plexus::shm)
endif()

# The MCAP examples are the analysis path (capture -> flat stream -> MCAP for Foxglove). They
# need the transcode, so they are built only when PLEXUS_BUILD_MCAP_TRANSCODE is on. The basic
# example produces JSON payloads with a declared jsonschema (decodes out of the box); the
# opaque example hands plexus a finished blob and supplies a matching schema separately.
if(TARGET mcap_transcode_lib)
    plexus_example(mcap_basic_foxglove plexus::plexus plexus::inproc)
    target_link_libraries(mcap_basic_foxglove PRIVATE mcap_transcode_lib)
    plexus_example(mcap_opaque_supplied_schema plexus::plexus plexus::inproc)
    target_link_libraries(mcap_opaque_supplied_schema PRIVATE mcap_transcode_lib)
endif()
