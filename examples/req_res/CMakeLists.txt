# Request/response over the asio TCP backend with native multicast discovery, in both the typed
# and the raw-bytes flavors. Each pair is a two-terminal example (server + client).
if(TARGET plexus::asio)
    plexus_example(bytes_reqres_server plexus::plexus plexus::asio)
    plexus_example(bytes_reqres_client plexus::plexus plexus::asio)
    plexus_example(typed_reqres_server plexus::plexus plexus::asio)
    plexus_example(typed_reqres_client plexus::plexus plexus::asio)
endif()
