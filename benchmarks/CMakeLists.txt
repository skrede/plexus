# Standalone benchmarks. Each subdirectory owns one probe binary built against the
# public API; none is a ctest. This directory is added only when PLEXUS_BUILD_BENCHMARKS
# is on, so the default tree never compiles it.

add_subdirectory(scalability)
