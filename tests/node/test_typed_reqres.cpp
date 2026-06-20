#include "test_typed_reqres_common.h"

using namespace typed_reqres_fixture;

TEST_CASE("typed reqres: round-trip decodes the handler's response value", "[node][typed][call]")
{
    net n;
    n.connect();

    typed_procedure proc{n.b, "rpc",
                         [](const request_t &req) -> plexus::expected<response_t, std::error_code>
                         { return response_t{req.value * 2}; }};
    typed_caller    call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(request_t{21},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got.has_value());
    REQUIRE(*got == 42);
}

TEST_CASE("typed reqres: a provider handler error preserves its value under provider_category",
          "[node][typed][call]")
{
    net n;
    n.connect();

    constexpr int   k_provider_value = 77;
    typed_procedure proc{n.b, "rpc",
                         [](const request_t &) -> plexus::expected<response_t, std::error_code>
                         {
                             return plexus::expected<response_t, std::error_code>{
                                     plexus::unexpect,
                                     std::error_code{k_provider_value, plexus::call_category()}};
                         }};
    typed_caller    call{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    call.call(request_t{1},
              [&](plexus::expected<response_t, std::error_code> r)
              {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(err->value() == k_provider_value);
    REQUIRE(&err->category() == &plexus::provider_category());
}

TEST_CASE("typed reqres: a provider request-decode failure surfaces as deserialize_failed",
          "[node][typed][call]")
{
    net n;
    n.connect();

    bool            handler_ran = false;
    typed_procedure proc{n.b, "rpc",
                         [&](const request_t &) -> plexus::expected<response_t, std::error_code>
                         {
                             handler_ran = true;
                             return response_t{0};
                         }};
    // A BYTES caller sends a malformed (non-4-byte) request to the typed procedure.
    plexus::caller<> raw{n.a, "rpc"};
    n.drive();

    std::optional<std::error_code> err;
    raw.call(as_bytes(std::string{"xyz"}),
             [&](plexus::expected<plexus::reply, std::error_code> r)
             {
                 REQUIRE_FALSE(static_cast<bool>(r));
                 err = r.error();
             });
    n.drive();
    REQUIRE(err.has_value());
    REQUIRE(*err == plexus::call_errc::deserialize_failed);
    REQUIRE_FALSE(handler_ran);
}
