// The stream read-buffer tunability oracle: a value-level check of the per-read buffer
// sizing the asio stream channels apply at construction. Proves the default stays exactly
// 64 KiB (so TCP/AF_UNIX/TLS channels are byte-identical when the size is omitted), that a
// custom size is honored verbatim (the constrained target dials it down), and that a
// degenerate (sub-floor / zero) request fails closed to the defined minimum rather than a
// silent zero-size buffer. No socket — the sizing is a pure function the ctors call, proven
// here at the value level; linked against plexus::asio for the constants + the helper.

#include "plexus/asio/stream_channel.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

using plexus::asio::k_min_stream_read_buffer_bytes;
using plexus::asio::k_stream_read_buffer_bytes;
using plexus::asio::stream_read_buffer_size;

TEST_CASE("stream_read_buffer default is the byte-identical 64 KiB", "[asio][stream_read_buffer]")
{
    REQUIRE(k_stream_read_buffer_bytes == 64u * 1024u);
    // Omitting the size (the ctor default) resolves to the unchanged 64 KiB.
    REQUIRE(stream_read_buffer_size(k_stream_read_buffer_bytes) == 64u * 1024u);
}

TEST_CASE("stream_read_buffer honors a custom at-or-above size", "[asio][stream_read_buffer]")
{
    // The MCU dials it down to a small-but-usable buffer; a large override is kept too.
    REQUIRE(stream_read_buffer_size(k_min_stream_read_buffer_bytes) == k_min_stream_read_buffer_bytes);
    REQUIRE(stream_read_buffer_size(8u * 1024u) == 8u * 1024u);
    REQUIRE(stream_read_buffer_size(256u * 1024u) == 256u * 1024u);
}

TEST_CASE("stream_read_buffer fails closed on a degenerate size", "[asio][stream_read_buffer]")
{
    // A degenerate (0) request must NEVER yield a zero-size buffer (a zero-length async_read
    // makes no progress); it floors to the defined minimum — never silently zero.
    REQUIRE(stream_read_buffer_size(0) == k_min_stream_read_buffer_bytes);
    REQUIRE(stream_read_buffer_size(0) > 0);
    // A sub-floor non-zero request floors too.
    REQUIRE(stream_read_buffer_size(k_min_stream_read_buffer_bytes - 1)
            == k_min_stream_read_buffer_bytes);
}
