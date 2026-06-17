# Host-side tools that link optional dependencies kept out of the core. Each tool
# self-gates on its own option at the root; this directory is only added when at
# least one tool is enabled.

add_subdirectory(mcap_transcode)
