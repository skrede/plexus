# A single node built with no caller transport, self-delivering in-process. Header-only inproc
# backend, so it builds unconditionally and runs as a single self-contained binary.
plexus_example(intra_node_self_delivery plexus::plexus plexus::inproc plexus::discovery)
