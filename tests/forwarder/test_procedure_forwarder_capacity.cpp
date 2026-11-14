#include "test_procedure_forwarder_common.h"

#include "plexus/io/node_name.h"

#include "plexus/node_id.h"

using namespace procedure_forwarder_fixture;

namespace {

constexpr std::size_t k_cap = 2;

const plexus::node_id k_responder{std::byte{0x0A}};
const plexus::node_id k_other{std::byte{0x0B}};

struct completion
{
    std::uint32_t runs{0};
    rpc_status status{rpc_status::success};
};

procedure_forwarder::on_response_fn recorder(completion &c)
{
    return [&c](rpc_status status, std::span<const std::byte>)
    {
        ++c.runs;
        c.status = status;
    };
}

struct capped_forwarder
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_channel<> ch{ex};
    capture cap{ex};
    plexus::log::null_logger sink;
    procedure_forwarder fwd{ex, k_long_deadline, sink, k_cap};
    procedure_forwarder::peer responder = make_peer(ch, cap, plexus::io::node_name_of(k_responder));
};

}

TEST_CASE("per-peer cap: direct calls holding the cap refuse a forwarded call to the same peer with error", "[procedure][capacity]")
{
    capped_forwarder f;
    completion held;
    completion refused;
    for(std::size_t i = 0; i < k_cap; ++i)
        f.fwd.call(f.responder, "svc", {}, recorder(held));

    f.fwd.forward_call(k_responder, "svc", {}, recorder(refused));

    CHECK(held.runs == 0);
    CHECK(refused.runs == 1);
    CHECK(refused.status == rpc_status::error);
    CHECK(f.fwd.outstanding() == k_cap);
}

TEST_CASE("per-peer cap: forwarded calls holding the cap refuse a direct call to the same peer with error", "[procedure][capacity]")
{
    capped_forwarder f;
    completion held;
    completion refused;
    for(std::size_t i = 0; i < k_cap; ++i)
        f.fwd.forward_call(k_responder, "svc", {}, recorder(held));

    f.fwd.call(f.responder, "svc", {}, recorder(refused));

    CHECK(held.runs == 0);
    CHECK(refused.runs == 1);
    CHECK(refused.status == rpc_status::error);
    CHECK(f.fwd.outstanding() == k_cap);
}

TEST_CASE("per-peer cap: a mixed direct and forwarded load at the cap leaves another peer's calls admitted", "[procedure][capacity]")
{
    capped_forwarder f;
    completion held;
    completion other;
    f.fwd.call(f.responder, "svc", {}, recorder(held));
    f.fwd.forward_call(k_responder, "svc", {}, recorder(held));

    f.fwd.forward_call(k_other, "svc", {}, recorder(other));

    CHECK(held.runs == 0);
    CHECK(other.runs == 0);
    CHECK(f.fwd.outstanding() == k_cap + 1);
}
