#include "test_session_gate_common.h"

#include "plexus/io/node_name.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using namespace session_gate_fixture;
using plexus::node_id;
using plexus::io::detail::drop_cause;

namespace {

const node_id k_origin    = make_cfg(0x03).self_id;
const node_id k_elsewhere = make_cfg(0x04).self_id;

struct completion
{
    std::uint32_t runs{0};
    rpc_status status{rpc_status::success};
};

rpc_forwarder::on_response_fn recorder(completion &c)
{
    return [&c](rpc_status status, std::span<const std::byte>)
    {
        ++c.runs;
        c.status = status;
    };
}

rpc_forwarder::peer peer_named(const established_acceptor &f, const node_id &id)
{
    return rpc_forwarder::peer{f.subject->rpc_peer().channel, plexus::io::node_name_of(id)};
}

std::vector<std::byte> envelope(const node_id &origin, const node_id &destination, std::vector<std::byte> inner)
{
    const wire::forwarded_frame ff{.origin = origin, .destination = destination, .hop = 1, .seq = 0, .flags = 0, .inner = std::move(inner)};
    return frame_of(wire::msg_type::forwarded, wire::encode_forwarded_frame(ff));
}

std::vector<std::byte> response_body(std::uint64_t correlation_id)
{
    const wire::bidirectional_header hdr{.source         = wire::endpoint_source_type::procedure,
                                         .sequence       = 0,
                                         .topic_hash     = wire::fqn_topic_hash(k_procedure),
                                         .type_hash_1    = 0,
                                         .type_hash_2    = 0,
                                         .correlation_id = correlation_id};
    return wire::encode_rpc_response(hdr, rpc_status::success, {});
}

std::vector<std::byte> response_frame(std::uint64_t correlation_id)
{
    return frame_of(wire::msg_type::rpc_response, response_body(correlation_id));
}

}

TEST_CASE("forwarded rpc: a response addressed to another node is dropped and counted on a node that cannot forward", "[integration][forwarded_rpc]")
{
    completion direct;
    established_acceptor f;
    f.procedures.call(peer_named(f, k_origin), k_procedure, {}, recorder(direct));

    f.inject(envelope(k_origin, k_elsewhere, response_frame(1)));

    CHECK(direct.runs == 0);
    CHECK(f.fx.drops == std::vector{drop_cause::unroutable});
    CHECK(f.subject->is_complete());
    CHECK(f.procedures.outstanding() == 1);
}

TEST_CASE("forwarded rpc: a request addressed to another node is dropped and counted on a node that cannot forward", "[integration][forwarded_rpc]")
{
    established_acceptor f;

    f.inject(envelope(k_origin, k_elsewhere, rpc_request_frame(0)));

    CHECK(f.fx.handler_runs == 0);
    CHECK(f.fx.drops == std::vector{drop_cause::unroutable});
    CHECK(f.subject->is_complete());
}

TEST_CASE("forwarded rpc: a forwarded response cannot complete a direct call's correlation", "[integration][forwarded_rpc]")
{
    completion direct;
    established_acceptor f;
    const rpc_forwarder::peer origin_peer = peer_named(f, k_origin);
    f.procedures.call(origin_peer, k_procedure, {}, recorder(direct));

    f.inject(envelope(k_origin, {}, response_frame(1)));

    CHECK(direct.runs == 0);
    CHECK(f.procedures.outstanding() == 1);
    CHECK(f.fx.drops.empty());

    f.procedures.deliver_response(origin_peer, response_body(1));

    CHECK(direct.runs == 1);
    CHECK(f.procedures.outstanding() == 0);
}

TEST_CASE("forwarded rpc: a direct response cannot complete a forwarded call's correlation", "[integration][forwarded_rpc]")
{
    completion forwarded;
    established_acceptor f;
    f.procedures.forward_call(k_origin, k_procedure, {}, recorder(forwarded));

    f.procedures.deliver_response(peer_named(f, k_origin), response_body(1));

    CHECK(forwarded.runs == 0);
    CHECK(f.procedures.outstanding() == 1);

    f.inject(envelope(k_origin, {}, response_frame(1)));

    CHECK(forwarded.runs == 1);
    CHECK(forwarded.status == rpc_status::success);
    CHECK(f.procedures.outstanding() == 0);
}

TEST_CASE("forwarded rpc: a detached peer drains its direct and forwarded calls", "[integration][forwarded_rpc]")
{
    completion direct;
    completion forwarded;
    established_acceptor f;
    const rpc_forwarder::peer origin_peer = peer_named(f, k_origin);
    f.procedures.call(origin_peer, k_procedure, {}, recorder(direct));
    f.procedures.forward_call(k_origin, k_procedure, {}, recorder(forwarded));

    f.procedures.detach_all(origin_peer);

    CHECK(direct.runs == 1);
    CHECK(direct.status == rpc_status::peer_disconnected);
    CHECK(forwarded.runs == 1);
    CHECK(forwarded.status == rpc_status::peer_disconnected);
    CHECK(f.procedures.outstanding() == 0);
}

TEST_CASE("forwarded data: a relayed publish for a topic this node never demanded from the relay is refused as unsolicited", "[integration][forwarded_data]")
{
    established_acceptor f;

    f.inject(envelope(k_origin, {}, data_frame("relayed")));

    CHECK(f.fx.messages == 0);
    CHECK(f.fx.drops == std::vector{drop_cause::unsolicited});
    const auto sent = wire::decode_header(f.last_peer_frame);
    REQUIRE(sent);
    CHECK(sent->type == wire::msg_type::unsubscribe);
}

TEST_CASE("forwarded data: a relayed publish for a demanded topic is delivered stamped with its origin", "[integration][forwarded_data]")
{
    std::vector<node_id> sources;
    established_acceptor f;
    f.messages.set_on_data_stamp([&sources](const node_id &source, std::uint64_t) { sources.push_back(source); });
    f.messages.attach(f.subject->msg_peer(), k_topic);

    f.inject(envelope(k_origin, {}, data_frame("relayed")));

    CHECK(f.fx.messages == 1);
    CHECK(sources == std::vector{k_origin});
    CHECK(f.fx.drops.empty());
}
