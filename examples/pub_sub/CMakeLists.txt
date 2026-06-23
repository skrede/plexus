# Pub/sub over the asio TCP backend with mDNS discovery, in both the typed and the raw-bytes
# flavors. Each pair is a two-terminal example (publisher + subscriber).
if(TARGET plexus::asio AND TARGET plexus::mdnspp)
    plexus_example(bytes_pubsub_pub plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(bytes_pubsub_sub plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_pubsub_pub plexus::plexus plexus::asio plexus::mdnspp)
    plexus_example(typed_pubsub_sub plexus::plexus plexus::asio plexus::mdnspp)
endif()
