# The in-process zero-serialization fast path. Header-only inproc backend, so it builds
# unconditionally and runs as a single self-contained binary.
plexus_example(typed_inproc_fastpath plexus::plexus plexus::inproc)
