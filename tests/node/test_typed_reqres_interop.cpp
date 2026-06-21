#include "test_typed_reqres_common.h"

using namespace typed_reqres_fixture;

TEST_CASE(
        "typed reqres: a typed caller against a bytes procedure replying error falls back to error",
        "[node][typed][call]")
{
    net n;
    n.connect();

    // A BYTES procedure replies a bare rpc_status::error with no varint payload.
    bytes_procedure proc{n.b, "rpc",
                         [](std::span<const std::byte>, bytes_procedure::reply_fn &reply)
                         { reply(plexus::wire::rpc_status::error, {}); }};
    typed_caller    call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{5},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::error);
}

TEST_CASE("typed reqres: a reply-decode failure surfaces as deserialize_failed",
          "[node][typed][call]")
{
    net n;
    n.connect();

    // A BYTES procedure replies success with a malformed (non-4-byte) response payload.
    bytes_procedure proc{
            n.b, "rpc", [](std::span<const std::byte>, bytes_procedure::reply_fn &reply)
            { reply(plexus::wire::rpc_status::success, as_bytes(std::string{"no"})); }};
    typed_caller call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{9},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::deserialize_failed);
}

TEST_CASE("typed reqres: a second local serve on one fqn throws on the typed form",
          "[node][typed][call]")
{
    net n;
    n.connect();

    typed_procedure proc{n.b, "rpc",
                         [](const request_t &) -> plexus::expected<response_t, std::error_code>
                         { return response_t{0}; }};

    REQUIRE_THROWS_AS(
            (typed_procedure{n.b, "rpc",
                             [](const request_t &) -> plexus::expected<response_t, std::error_code>
                             { return response_t{1}; }}),
            std::runtime_error);
}

#ifdef PLEXUS_HAS_FAMILY_SPELLING
// The codec-family spelling assertions. A symmetric family slot expands one class-template
// codec family to the per-half codecs Family<Req> / Family<Res> over a Res(Req) signature;
// u32_codec is exactly such a family. The symmetric form must name the SAME endpoint type as
// the explicit per-half expansion (the response family defaults to the request family).
static_assert(__is_same(plexus::procedure<response_t(request_t), u32_codec>,
                        plexus::procedure<response_t(request_t), u32_codec, u32_codec>));
static_assert(__is_same(plexus::caller<response_t(request_t), u32_codec>,
                        plexus::caller<response_t(request_t), u32_codec, u32_codec>));

TEST_CASE("typed reqres: the family-form spelling round-trips the typed reqres",
          "[node][typed][call][family]")
{
    using family_procedure = plexus::procedure<response_t(request_t), u32_codec>;
    using family_caller    = plexus::caller<response_t(request_t), u32_codec>;

    net n;
    n.connect();

    family_procedure proc{n.b, "rpc",
                          [](const request_t &req) -> plexus::expected<response_t, std::error_code>
                          { return response_t{req.value * 2}; }};
    family_caller    call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(request_t{21},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got == 42u);
}
#endif
