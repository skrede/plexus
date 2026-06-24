#include "test_procedure_forwarder_common.h"

#include "support/alloc_counter.h"

using namespace procedure_forwarder_fixture;

namespace {

// A non-allocating sink Policy: its byte_channel records send sizes without
// copying, so a forwarder<sink_policy> call/reply exercises framing + dispatch
// with no transport-side allocation — isolating the forwarder's own heap behavior.
struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &)
    {
    }
    sink_channel(sink_executor &, std::error_code &)
    {
    }

    void send(std::span<const std::byte> d)
    {
        total_bytes += d.size();
        ++sends;
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &)
    {
    }
    sink_timer(sink_executor &, std::error_code &)
    {
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }
    void cancel()
    {
    }
};

struct sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::Policy<sink_policy>);

}

TEST_CASE("steady-state provider dispatch + reply framing allocates nothing", "[procedure]")
{
    // The provider receive tail is the req/res hot path: decode an inbound request,
    // dispatch to the handler over opaque bytes, and frame the reply into reused
    // scratch. It performs NO map insertion (the handler registry is grown at
    // serve()), so after warm-up it must allocate nothing — the sibling property of
    // message_forwarder's frame-once fan-out. Measured over the non-allocating sink
    // Policy so the only heap traffic that could appear is the forwarder's own.
    using sink_forwarder = plexus::io::procedure_forwarder<sink_policy>;

    sink_executor ex;
    sink_channel provider_ch(ex);
    plexus::log::null_logger log_sink;
    sink_forwarder provider{ex, k_long_deadline, log_sink};
    sink_forwarder::peer provider_peer{provider_ch, "caller-node"};

    // A handler that replies with a fixed return span captured by reference: the
    // reply itself moves no heap (the body lives in the test frame).
    const std::string ret_body = "return-bytes";
    provider.serve("svc", [&](std::span<const std::byte>, sink_forwarder::reply_fn &reply) { reply(rpc_status::success, as_bytes(ret_body)); });

    // The inbound request inner (header-off), built ONCE outside the gate and reused
    // every iteration, so the gate measures only the forwarder's decode + dispatch +
    // reply-framing path.
    const std::string param = "steady-param";
    plexus::wire::bidirectional_header req_hdr{.source         = plexus::wire::endpoint_source_type::caller,
                                               .sequence       = 0,
                                               .topic_hash     = plexus::wire::fqn_topic_hash("svc"),
                                               .type_hash_1    = 0,
                                               .type_hash_2    = 0,
                                               .correlation_id = 1};
    std::vector<std::byte> req_inner;
    plexus::wire::encode_rpc_request_into(req_inner, req_hdr, as_bytes(param));

    // Warm-up: one dispatch grows the reply + frame scratch to steady-state size.
    provider.deliver_request(provider_peer, req_inner);
    const auto sends_before = provider_ch.sends;

    constexpr int K = 256;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        provider.deliver_request(provider_peer, req_inner);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(provider_ch.sends - sends_before == K); // every dispatch replied
    REQUIRE(after - before == 0);                   // decode + dispatch + reply: zero alloc
}
