#include "test_call_family_common.h"

using namespace call_family_fixture;

TEST_CASE("call family: round-trip is byte-identical, looped", "[node][call]")
{
    constexpr int k_iterations = 5;
    net           n;
    n.connect();

    // The provider echoes "reply:<param>".
    inproc_procedure proc{n.b, "rpc",
                          [](std::span<const std::byte> param, inproc_procedure::reply_fn &reply)
                          {
                              const std::string out = "reply:" + to_string(param);
                              reply(plexus::wire::rpc_status::success, as_bytes(out));
                          }};
    inproc_caller    call{n.a, "rpc"};
    n.drive();

    int delivered = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const std::string              req = "req-" + std::to_string(i);
        std::optional<std::string>     got;
        std::optional<plexus::node_id> provider;
        bool                           reception_stamped = false;
        call.call(as_bytes(req),
                  [&](plexus::expected<reply_t, std::error_code> r)
                  {
                      REQUIRE(static_cast<bool>(r));
                      got = to_string(r.value().bytes);
                      if(r.value().info.provider_identity)
                          provider = r.value().info.provider_identity->node_id();
                      reception_stamped = r.value().info.reception_timestamp != 0;
                  });
        n.drive();
        REQUIRE(got.has_value());
        REQUIRE(*got == "reply:" + req);
        REQUIRE(provider.has_value());
        REQUIRE(*provider == n.id_b);
        REQUIRE(reception_stamped);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("call family: a second local serve on one fqn throws and leaves the first serving",
          "[node][call]")
{
    net n;
    n.connect();

    int              first_calls = 0;
    inproc_procedure proc{
            n.b, "rpc", [&](std::span<const std::byte>, inproc_procedure::reply_fn &reply)
            {
                ++first_calls;
                reply(plexus::wire::rpc_status::success, as_bytes(std::string{"first"}));
            }};

    REQUIRE_THROWS_AS(
            (inproc_procedure{n.b, "rpc",
                              [](std::span<const std::byte>, inproc_procedure::reply_fn &reply)
                              { reply(plexus::wire::rpc_status::success, {}); }}),
            std::runtime_error);

    // The refused ctor left no side effect: the FIRST handler still answers.
    inproc_caller call{n.a, "rpc"};
    n.drive();
    std::optional<std::string> got;
    call.call(as_bytes(std::string{"x"}),
              [&](plexus::expected<reply_t, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  got = to_string(r.value().bytes);
              });
    n.drive();
    REQUIRE(got == "first");
    REQUIRE(first_calls == 1);
}

TEST_CASE("call family: dropping the procedure retires it to no_handler", "[node][call]")
{
    net n;
    n.connect();

    {
        inproc_procedure proc{
                n.b, "rpc", [](std::span<const std::byte>, inproc_procedure::reply_fn &reply)
                { reply(plexus::wire::rpc_status::success, as_bytes(std::string{"served"})); }};
        n.drive();

        inproc_caller              call{n.a, "rpc"};
        std::optional<std::string> got;
        call.call(as_bytes(std::string{"q"}),
                  [&](plexus::expected<reply_t, std::error_code> r)
                  {
                      REQUIRE(static_cast<bool>(r));
                      got = to_string(r.value().bytes);
                  });
        n.drive();
        REQUIRE(got == "served");
    }
    // proc dropped here — its handler is retired.

    inproc_caller                  call{n.a, "rpc"};
    std::optional<std::error_code> err;
    plexus::call_options           opts;
    opts.deadline = std::chrono::milliseconds(50);
    call.call(as_bytes(std::string{"q"}), opts,
              [&](plexus::expected<reply_t, std::error_code> r)
              {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::no_handler);
}
