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

plexus_example(typed_inproc_fastpath plexus::plexus plexus::inproc)
