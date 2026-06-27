# Pub/sub over the asio TCP backend with native multicast discovery, in both the typed and the
# raw-bytes flavors. Each pair is a two-terminal example (publisher + subscriber).
if(TARGET plexus::asio)
    plexus_example(bytes_pubsub_pub plexus::plexus plexus::asio)
    plexus_example(bytes_pubsub_sub plexus::plexus plexus::asio)
    plexus_example(typed_pubsub_pub plexus::plexus plexus::asio)
    plexus_example(typed_pubsub_sub plexus::plexus plexus::asio)
endif()
