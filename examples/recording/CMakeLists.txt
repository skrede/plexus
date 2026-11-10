# The capture leg builds unconditionally. The MCAP examples and the in-process flat-to-MCAP step need
# mcap_transcode_lib, which exists only when PLEXUS_BUILD_MCAP_TRANSCODE is on.
plexus_example(recording_example plexus::plexus plexus::inproc plexus::discovery)
if(TARGET mcap_transcode_lib)
    target_link_libraries(recording_example PRIVATE mcap_transcode_lib)
    target_compile_definitions(recording_example PRIVATE PLEXUS_WITH_MCAP_TRANSCODE)

    plexus_example(mcap_basic_foxglove plexus::plexus plexus::inproc plexus::discovery)
    target_link_libraries(mcap_basic_foxglove PRIVATE mcap_transcode_lib plexus::hints-robotics plexus::recording-host)
    plexus_example(mcap_opaque_supplied_schema plexus::plexus plexus::inproc plexus::discovery)
    target_link_libraries(mcap_opaque_supplied_schema PRIVATE mcap_transcode_lib)
endif()
