# Zero-config discovery over the asio backend: default_discovery composes the multicast socket
# and the first-party discovery leaf in one line. Each binary is run twice (plain, then with the
# `second` argument) in two shells on one host or two hosts on one LAN.
if(TARGET plexus::asio)
    plexus_example(discovery_hello plexus::plexus plexus::asio)
    plexus_example(discovery_fleet plexus::plexus plexus::asio)
endif()
