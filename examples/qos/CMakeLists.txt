# Static discovery is in the core, so the asio backend is the only dependency.
if(TARGET plexus::asio)
    plexus_example(size_envelope_qos plexus::plexus plexus::asio)
endif()
