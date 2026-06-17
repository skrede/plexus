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
