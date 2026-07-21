// A forwarded request whose handler DEFERS its reply must route the reply back to ITS OWN requester, not
// to whichever request most recently ran. The reply context is bound per dispatch (captured by value into
// the reply closure), so two interleaved forwarded requests — both deferred, then answered in order — each
// re-address the forwarded reply to the correct origin with the correct correlation_id. A shared single-slot
// reply-origin would route both replies to the second requester; this pins that it does not.

#include "test_procedure_forwarder_common.h"

#include "plexus/node_id.h"

using namespace procedure_forwarder_fixture;
using plexus::node_id;

namespace {

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

std::vector<std::byte> request_inner(std::string_view fqn, std::uint64_t corr_id, std::string_view param)
{
    plexus::wire::bidirectional_header hdr{.source         = plexus::wire::endpoint_source_type::caller,
                                           .sequence       = 0,
                                           .topic_hash     = plexus::wire::fqn_topic_hash(fqn),
                                           .type_hash_1    = 0,
                                           .type_hash_2    = 0,
                                           .correlation_id = corr_id};
    std::vector<std::byte> out;
    plexus::wire::encode_rpc_request_into(out, hdr, as_bytes(std::string{param}));
    return out;
}

std::uint64_t reply_correlation(std::span<const std::byte> forwarded_frame)
{
    const auto inner = forwarded_frame.subspan(plexus::wire::header_size);
    auto decoded     = plexus::wire::decode_rpc_response(inner);
    return decoded ? decoded->header.correlation_id : 0;
}

}

TEST_CASE("procedure_forwarder deferred reply: two interleaved forwarded requests each route back to their "
          "own requester",
          "[procedure][forward][deferred]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    plexus::log::null_logger sink;
    procedure_forwarder relay{ex, k_long_deadline, sink};

    inproc_channel<> arrival_tx{ex};
    capture arrival_cap{ex};
    procedure_forwarder::peer arrival = make_peer(arrival_tx, arrival_cap, "upstream");

    // The engine seam a forwarded reply leaves through: record (destination, frame) per emission.
    std::vector<std::pair<node_id, std::vector<std::byte>>> forwarded;
    relay.on_forward_rpc(
            [&](const node_id &dest, std::span<const std::byte> frame)
            {
                forwarded.emplace_back(dest, std::vector<std::byte>(frame.begin(), frame.end()));
                return true;
            });

    // A handler that DEFERS: it moves its reply out to answer later, holding no reference to relay state.
    std::vector<procedure_forwarder::reply_fn> deferred;
    relay.serve("svc", [&](std::span<const std::byte>, procedure_forwarder::reply_fn &reply) { deferred.push_back(std::move(reply)); });

    const node_id origin_a = id_of(0xA0);
    const node_id origin_b = id_of(0xB0);

    relay.deliver_request(arrival, request_inner("svc", 100, "a"), /*session_id=*/0, &origin_a);
    relay.deliver_request(arrival, request_inner("svc", 200, "b"), /*session_id=*/0, &origin_b);
    REQUIRE(deferred.size() == 2u);

    deferred[0](rpc_status::success, {}); // answer request A
    deferred[1](rpc_status::success, {}); // answer request B

    REQUIRE(forwarded.size() == 2u);
    REQUIRE(forwarded[0].first == origin_a);                  // A's reply addressed to A's origin
    REQUIRE(reply_correlation(forwarded[0].second) == 100u);
    REQUIRE(forwarded[1].first == origin_b);                  // B's reply addressed to B's origin
    REQUIRE(reply_correlation(forwarded[1].second) == 200u);
}
