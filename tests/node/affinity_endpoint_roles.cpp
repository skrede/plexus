#include "affinity_roles.h"
#include "affinity_trip_child_common.h"

#include "in_memory_byte_sink.h"

#include "plexus/recorder.h"
#include "plexus/recorder_options.h"

#include <optional>
#include <string_view>

namespace plexus::testing {

namespace {

using asio_recorder = plexus::recorder<affinity_asio_node::engine_type, affinity_asio_node::policy_type>;

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
    if(run_recorder_role(role))
        return role_outcome::not_tripped;
    return role_outcome::unknown;
}

}
