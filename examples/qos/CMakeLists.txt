# The size-envelope QoS example. Needs only the asio backend (static discovery is in the core),
# so it is guarded on asio alone rather than the asio+mDNS pair the networked examples use.
if(TARGET plexus::asio)
    plexus_example(size_envelope_qos plexus::plexus plexus::asio)
endif()
