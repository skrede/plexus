# Request/response over the asio TCP backend with mDNS discovery, in both the typed and the
# raw-bytes flavors. Each pair is a two-terminal example (server + client).
if(TARGET plexus::asio AND TARGET plexus::mdnspp)
    plexus_example(bytes_reqres_server plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(bytes_reqres_client plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_reqres_server plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_reqres_client plexus::plexus plexus::asio plexus::mdnspp)
endif()
