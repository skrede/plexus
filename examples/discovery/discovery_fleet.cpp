// The two knobs on top of the zero-config discovery composition: a universe assignment partitions
// the discovery plane (a node in another universe never rendezvous with these), and a liveliness
// observer receives the fused peer-alive / peer-lost verdicts. Run the binary twice
// (`discovery_fleet` and `discovery_fleet second`); each prints the peer's alive verdict. Then
// kill one instance: the survivor prints the lost verdict once the peer's awareness ages out.

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/observer.h"
#include "plexus/io/node_name.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/discovery/universe.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/default_discovery.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

class liveliness_printer : public plexus::io::observer
{
public:
    void on_peer_liveliness(const plexus::io::peer_liveliness_event &event) override
    {
        const bool alive = event.verdict == plexus::io::liveliness_verdict::alive;
        std::cout << "peer " << plexus::io::node_name_of(event.id) << (alive ? " alive" : " lost") << '\n';
    }

    bool observes_liveliness() const override
    {
        return true;
    }
};

plexus::node_options fleet_options(bool second)
{
    plexus::node_options opts;
    opts.name        = second ? "fleet-second" : "fleet-first";
    opts.reconnect   = plexus::io::reconnect_config{std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = second ? 0xF1EE72u : 0xF1EE71u;
    // Shortened from the 15s default so a killed peer's lost verdict lands within the demo's patience.
    opts.liveliness.awareness_ttl = std::chrono::seconds(3);
    return opts;
}

}

int main(int argc, char **argv)
{
    const bool second = argc > 1 && std::string_view{argv[1]} == "second";
    std::cout.setf(std::ios::unitbuf);

    asio::io_context io;
    plexus::discovery::discovery_options disc_opts;
    disc_opts.universe = plexus::discovery::universe_from_label("fleet-demo");
    plexus::asio::default_discovery discovery{io, disc_opts};
    plexus::asio::asio_transport transport{io};

    liveliness_printer watcher;
    const plexus::node_options opts = fleet_options(second);
    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{io, discovery.discovery(), opts.name, transport, opts};
    node.router().add_observer(watcher);
    node.listen({"tcp", second ? "0.0.0.0:5583" : "0.0.0.0:5582"});

    io.run();
}
