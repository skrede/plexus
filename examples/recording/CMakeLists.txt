# Capturing a session to a flat record stream over the public byte_sink seam, plus the two
# end-to-end MCAP examples (the analysis path: capture -> flat stream -> MCAP for Foxglove).
# The capture leg builds unconditionally; the MCAP examples and the in-process flat->MCAP
# step compile in only when PLEXUS_BUILD_MCAP_TRANSCODE is on (it defines mcap_transcode_lib).
plexus_example(recording_example plexus::plexus plexus::inproc plexus::discovery)
if(TARGET mcap_transcode_lib)
    target_link_libraries(recording_example PRIVATE mcap_transcode_lib)
    target_compile_definitions(recording_example PRIVATE PLEXUS_WITH_MCAP_TRANSCODE)

    plexus_example(mcap_basic_foxglove plexus::plexus plexus::inproc plexus::discovery)
    target_link_libraries(mcap_basic_foxglove PRIVATE mcap_transcode_lib plexus::hints-robotics plexus::recording-host)
    plexus_example(mcap_opaque_supplied_schema plexus::plexus plexus::inproc plexus::discovery)
    target_link_libraries(mcap_opaque_supplied_schema PRIVATE mcap_transcode_lib)
endif()
