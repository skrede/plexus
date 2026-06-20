#include "test_call_family_common.h"

using namespace call_family_fixture;

#ifdef PLEXUS_HAS_FAMILY_SPELLING
// The codec-family spelling positive assertions. A codec slot takes a class-template codec
// FAMILY; the typed endpoint applies Family<Req> / Family<Res> over a Res(Req) signature. The
// symmetric form (one family) defaults the response family to the request family; the
// asymmetric form spells two families, request-first. echo_codec is a trivial value codec
// family (one u32), supplied here since this TU defines no codec of its own.
namespace family_check {

struct u32_value
{
    std::uint32_t value{};
};

template<typename T>
struct echo_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        return plexus::wire_bytes<>{std::span<const std::byte>{owner->data(), owner->size()},
                                    std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, T &out) const
    {
        if(b.size() != 4)
            return plexus::expected<void, std::error_code>{
                    plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[i])) << (8 * i);
        out.value = v;
        return {};
    }
};

// The symmetric form defaults the response family to the request family: one spelled family
// names the same endpoint as the explicit two-family form over a single family.
static_assert(__is_same(plexus::procedure<u32_value(u32_value), echo_codec>,
                        plexus::procedure<u32_value(u32_value), echo_codec, echo_codec>));
static_assert(__is_same(plexus::caller<u32_value(u32_value), echo_codec>,
                        plexus::caller<u32_value(u32_value), echo_codec, echo_codec>));

// The asymmetric form binds two DIFFERENT families for request and response, request-first:
// the response half is alt_codec while the request half stays echo_codec, so the endpoint
// differs from the symmetric echo_codec spelling. A second codec family witnesses it.
template<typename T>
struct alt_codec
{
    using value_type = T;

    plexus::wire_bytes<> encode(const T &v) const { return echo_codec<T>{}.encode(v); }
    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, T &out) const
    {
        return echo_codec<T>{}.decode(b, out);
    }
};

using asymmetric_procedure = plexus::procedure<u32_value(u32_value), echo_codec, alt_codec>;
static_assert(__is_same(asymmetric_procedure,
                        plexus::procedure<u32_value(u32_value), echo_codec, alt_codec>));
static_assert(!__is_same(asymmetric_procedure,
                         plexus::procedure<u32_value(u32_value), echo_codec>));

// The one-off concrete-codec lift idiom: an alias template lifts a finished codec into the
// family slot (the P0522 template-template-argument path). It must bind and name the same
// endpoint as the direct family spelling.
template<typename>
using lifted_echo = echo_codec<u32_value>;
static_assert(__is_same(plexus::procedure<u32_value(u32_value), lifted_echo>,
                        plexus::procedure<u32_value(u32_value), lifted_echo, lifted_echo>));
static_assert(plexus::typed_codec<echo_codec<u32_value>>);
static_assert(plexus::typed_codec<alt_codec<u32_value>>);

}

TEST_CASE("call family: the family-form spelling names the same caller/procedure endpoint",
          "[node][call][family]")
{
    using family_check::u32_value;
    using family_procedure = plexus::procedure<u32_value(u32_value), family_check::echo_codec>;
    using family_caller    = plexus::caller<u32_value(u32_value), family_check::echo_codec>;

    net n;
    n.connect();

    family_procedure proc{n.b, "rpc",
                          [](const u32_value &req) -> plexus::expected<u32_value, std::error_code>
                          { return u32_value{req.value + 1}; }};
    family_caller    call{n.a, "rpc"};
    n.drive();

    std::optional<std::uint32_t> got;
    call.call(u32_value{41},
              [&](plexus::expected<u32_value, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  got = r.value().value;
              });
    n.drive();
    REQUIRE(got == 42u);
}
#endif
