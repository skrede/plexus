#include "plexus/io/shm/region_broker_concept.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/same_host.h"
#include "plexus/io/shm/shm_mux_member.h"
#include "plexus/io/shm/shm_slot_owner.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/scheduler_key.h"
#include "plexus/wire/stream_inbound.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// The shared-memory member joins the multiplexer as the SECOND same-host (local-tier)
// member alongside a stream member, so the local tier resolves to >1 candidate and the
// preference hook becomes load-bearing. This oracle proves: (1) the member satisfies the
// mux_member + byte_channel concepts; (2) over a live core composition the preference hook
// routes a same-host SHM-eligible dial to the SHM member when the ring acquires; and (3)
// when a forced broker failure makes the acquire fail, the SAME dial falls back to the
// stream member. Looped N>=100 in-body; the binary is re-run >=3 process runs.

using namespace plexus::io::shm;
namespace pio = plexus::io;

namespace {

// A heap-backed shared region store + the stub broker over it (the single-process
// stand-in for /dev/shm the registry oracle uses), extended with a fail switch: when
// armed, every create/attach returns failed, forcing the member's acquire to fail (the
// fallback path the preference hook must take).
struct region_store
{
    struct region
    {
        std::vector<std::byte> storage;
        std::byte             *base = nullptr;
        std::size_t            size = 0;
        bool                   live = false;
    };

    std::map<std::string, std::shared_ptr<region>> regions;

    std::shared_ptr<region> make(const std::string &name, std::size_t bytes)
    {
        auto r        = std::make_shared<region>();
        r->storage.assign(bytes + k_cache_line, std::byte{});
        auto raw      = reinterpret_cast<std::uintptr_t>(r->storage.data());
        auto algn     = (raw + k_cache_line - 1) & ~static_cast<std::uintptr_t>(k_cache_line - 1);
        r->base       = reinterpret_cast<std::byte *>(algn);
        r->size       = bytes;
        r->live       = true;
        regions[name] = r;
        return r;
    }
};

class stub_handle
{
public:
    stub_handle() = default;
    stub_handle(std::shared_ptr<region_store::region> region) noexcept : m_region(std::move(region)) {}
    stub_handle(const stub_handle &) = delete;
    stub_handle &operator=(const stub_handle &) = delete;
    stub_handle(stub_handle &&) = default;
    stub_handle &operator=(stub_handle &&) = default;

    std::span<std::byte> bytes() const { return {m_region->base, m_region->size}; }

private:
    std::shared_ptr<region_store::region> m_region;
};

class stub_broker
{
public:
    using region_handle = stub_handle;

    explicit stub_broker(region_store &store) noexcept : m_store(store) {}

    void fail(bool on) noexcept { m_fail = on; }

    region_status create(std::string_view name, std::size_t bytes, const create_options &,
                         region_handle &out)
    {
        if(m_fail)
            return region_status::failed;
        const std::string key{name};
        if(auto it = m_store.regions.find(key); it != m_store.regions.end() && it->second->live)
            return region_status::already_exists;
        out = stub_handle(m_store.make(key, bytes));
        return region_status::ok;
    }

    region_status attach(std::string_view name, region_handle &out)
    {
        if(m_fail)
            return region_status::failed;
        auto it = m_store.regions.find(std::string{name});
        if(it == m_store.regions.end() || !it->second->live)
            return region_status::not_found;
        out = stub_handle(it->second);
        return region_status::ok;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>) {}

private:
    region_store &m_store;
    bool          m_fail = false;
};

static_assert(region_broker<stub_broker>, "stub_broker must satisfy the region_broker seam");

// A no-op notifier (the deterministic single-process oracle drives no cross-process
// wake): default-constructible so the registry's default binder builds it.
struct stub_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(notifier<stub_notifier>, "stub_notifier must satisfy the notifier seam");

using shm_member = shm_mux_member<stub_broker, stub_notifier>;

static_assert(pio::byte_channel<shm_byte_channel<stub_broker, stub_notifier>>,
              "shm_byte_channel must satisfy byte_channel — the seven erased verbs");
static_assert(pio::mux_member<shm_member>,
              "shm_mux_member must satisfy mux_member — channel_type + mux_schemes + mux_tier");
static_assert(shm_member::mux_tier == pio::transport_kind::local,
              "the shm member rides the local (same-host) tier");
static_assert(shm_member::mux_prefers_shm,
              "the shm member opts into the per-candidate same-host fast-path flag");

// A dummy local-tier stream member (the AF_UNIX stand-in): it serves "shm" too so the two
// members are BOTH candidates for the same dialed endpoint (the multi-candidate-per-tier
// case the hook decides). It records whether the mux routed the dial to it.
struct dummy_stream_channel
{
    pio::endpoint ep;
    std::uint64_t key = plexus::io::detail::next_scheduler_key();
    void send(std::span<const std::byte>) {}
    void close() {}
    [[nodiscard]] pio::endpoint remote_endpoint() const { return ep; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(pio::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    [[nodiscard]] std::size_t backpressured() const noexcept { return 0; }
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return key; }
};

struct dummy_stream_member
{
    using channel_type = dummy_stream_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr pio::transport_kind mux_tier = pio::transport_kind::local;

    bool dialed = false;

    void listen(const pio::endpoint &) {}
    void dial(const pio::endpoint &) { dialed = true; }
    void close() {}
    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dummy_stream_channel>)>) {}
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dummy_stream_channel>, const pio::endpoint &)>) {}
    void on_dial_failed(plexus::detail::move_only_function<void(const pio::endpoint &, pio::io_error)>) {}
    void on_error(plexus::detail::move_only_function<void(pio::io_error)>) {}
};

// The composition: the SHM member FIRST (the preference hook scans for its shm_eligible
// flag), the stream member second (the fallback). Both serve the local tier + "shm", so a
// same-host dial resolves to both candidates and the hook decides.
using mux_t = pio::multiplexing_transport<shm_member, dummy_stream_member>;

}

TEST_CASE("shm.mux the preference hook routes a same-host dial to the SHM member when it acquires",
          "[shm][mux]")
{
    constexpr int k_iterations = 100;
    int routed_to_shm = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        region_store store;
        stub_broker  broker{store};
        shm_member   shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};

        dummy_stream_member stream;
        mux_t mux{shm, stream, {}, prefer_shm_hook(shm)};

        // The mux wraps each member's dialed channel into a polymorphic_byte_channel and
        // re-emits it; the dialed channel's scheme survives the erasure, so a "shm"-scheme
        // channel arriving here proves the SHM member served the dial.
        std::string dialed_scheme;
        mux.on_dialed([&](std::unique_ptr<pio::polymorphic_byte_channel> ch, const pio::endpoint &) {
            dialed_scheme = ch->remote_endpoint().scheme;
        });

        mux.dial({"shm", "topic.same_host"});

        // The ring acquired, so the preference hook routed to the SHM member — the dialed
        // channel reports the "shm" scheme — NOT the stream fallback (the dummy stream
        // member's dial fires no completion and sets its flag, which stays false here).
        REQUIRE(dialed_scheme == "shm");
        REQUIRE_FALSE(stream.dialed);
        ++routed_to_shm;
    }
    REQUIRE(routed_to_shm == k_iterations);
}

TEST_CASE("shm.mux the SAME dial falls back to the stream member when the ring acquire fails",
          "[shm][mux]")
{
    constexpr int k_iterations = 100;
    int fell_back = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        region_store store;
        stub_broker  broker{store};
        broker.fail(true); // force every ring acquire to fail

        shm_member shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};

        dummy_stream_member stream;
        mux_t mux{shm, stream, {}, prefer_shm_hook(shm)};

        std::string dialed_scheme;
        mux.on_dialed([&](std::unique_ptr<pio::polymorphic_byte_channel> ch, const pio::endpoint &) {
            dialed_scheme = ch->remote_endpoint().scheme;
        });

        mux.dial({"shm", "topic.same_host"});

        // The forced broker failure made can_acquire return false, so the preference hook
        // fell back to the stream member — the dummy stream member's dial fired (its flag
        // set), and the SHM member never served the dial (no "shm" channel arrived).
        REQUIRE(stream.dialed);
        REQUIRE(dialed_scheme.empty());
        ++fell_back;
    }
    REQUIRE(fell_back == k_iterations);
}

TEST_CASE("shm.mux a congestion-blocked send drives on_drop with cause=blocked transport=local",
          "[shm][mux][drop]")
{
    // The same-host SHM tier is not a dark drop tier: a send into a full ring (the reliable
    // gate finds no recyclable slot with no consumer draining) surfaces the verdict off
    // send() AND posts a drop_event{blocked, locality::local} through the channel's on_drop —
    // the edge the engine binds to its posted drop_sink at dial/accept. Loop so a single-run
    // fluke cannot pass.
    constexpr int k_iterations = 4;
    int saw_blocked = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        region_store store;
        stub_broker  broker{store};
        shm_member   shm{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};

        std::unique_ptr<shm_member::channel_type> ch;
        shm.on_dialed([&](std::unique_ptr<shm_member::channel_type> c, const pio::endpoint &) {
            ch = std::move(c);
        });
        shm.dial({"shm", "topic.blocked"});
        REQUIRE(ch);

        std::vector<plexus::io::detail::drop_event> drops;
        ch->on_drop([&drops](const plexus::io::detail::drop_event &ev) { drops.push_back(ev); });

        // No consumer cursor is registered, so the reliable ring fills and a send past its
        // depth finds no free slot — the congestion verdict fires emit_drop. Flood well past
        // any default depth.
        const std::vector<std::byte> payload(64, std::byte{0x5A});
        for(int i = 0; i < 4096 && drops.empty(); ++i)
            ch->send(std::span<const std::byte>{payload});

        REQUIRE_FALSE(drops.empty());
        for(const auto &ev : drops)
        {
            REQUIRE(ev.cause == plexus::io::detail::drop_cause::blocked);
            REQUIRE(ev.transport == plexus::io::locality::local);
        }
        ++saw_blocked;
    }
    REQUIRE(saw_blocked == k_iterations);
}
