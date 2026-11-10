#include "affinity_roles.h"
#include "affinity_trip_child_common.h"

#include "in_memory_byte_sink.h"

#include "plexus/expected.h"
#include "plexus/recorder.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/value_logger.h"
#include "plexus/recorder_options.h"
#include "plexus/value_logger_options.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::testing {

namespace {

using asio_recorder = plexus::recorder<affinity_asio_node::engine_type, affinity_asio_node::policy_type>;

struct tagged_codec
{
    using value_type = std::uint32_t;

    plexus::wire_bytes<> encode(const std::uint32_t &) const
    {
        return {};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, std::uint32_t &) const
    {
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0xA5u, "affinity_value"};
    }
};

// The recorder is made on the driver, so only the member called from main can trip.
template<typename Use>
void foreign_recorder(plexus::recording_mode mode, Use use)
{
    asio_rig rig;
    in_memory_byte_sink sink;
    std::optional<asio_recorder> rec;
    on_driver(rig.node, [&] { rec.emplace(rig.node.make_recorder(sink, plexus::recorder_options{.mode = mode})); });
    use(*rec);
}

// The handle is made on the driver, so only the decode_failed read from main can trip.
template<typename Handle, typename Make>
void foreign_decode_failed(Make make)
{
    asio_rig rig;
    std::optional<Handle> handle;
    on_driver(rig.node, [&] { handle.emplace(make(rig.node)); });
    static_cast<void>(handle->decode_failed());
}

bool run_decode_failed_role(std::string_view role)
{
    std::ostringstream out;
    if(role == "foreign_subscriber_decode_failed")
        foreign_decode_failed<plexus::subscriber<tagged_codec>>([](affinity_asio_node &n)
                                                                { return plexus::subscriber<tagged_codec>{n, "affinity/typed", [](const std::uint32_t &) {}}; });
    else if(role == "foreign_value_logger_decode_failed")
        foreign_decode_failed<plexus::value_logger<tagged_codec>>([&](affinity_asio_node &n)
                                                                  { return n.log<tagged_codec>("affinity/typed", plexus::value_logger_options{.out = out}); });
    else
        return false;
    return true;
}

bool run_recorder_role(std::string_view role)
{
    using plexus::recording_mode;
    if(role == "foreign_recorder_pump")
        foreign_recorder(recording_mode::continuous, [](asio_recorder &rec) { rec.pump(); });
    else if(role == "foreign_recorder_flush")
        foreign_recorder(recording_mode::continuous, [](asio_recorder &rec) { rec.flush(); });
    else if(role == "foreign_recorder_trigger")
        foreign_recorder(recording_mode::pre_buffer, [](asio_recorder &rec) { rec.trigger(); });
    else if(role == "foreign_recorder_captured_span")
        foreign_recorder(recording_mode::pre_buffer, [](asio_recorder &rec) { rec.captured_span(); });
    else if(role == "foreign_recorder_preamble")
        foreign_recorder(recording_mode::continuous, [](asio_recorder &rec) { rec.preamble(); });
    else
        return false;
    return true;
}

}

role_outcome run_endpoint_role(std::string_view role)
{
    if(run_recorder_role(role) || run_decode_failed_role(role))
        return role_outcome::not_tripped;
    return role_outcome::unknown;
}

}
