// Bytes request/response — the server half. Serves an "uppercase" procedure that
// transforms the request bytes and replies them. Run alongside bytes_reqres_client.

#include "plexus/node.h"
#include "plexus/procedure.h"
#include "plexus/node_options.h"

#include "plexus/wire/rpc_status.h"

#include "plexus/discovery/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <optional>

int main()
{
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::asio::udp_multicast_socket mc_socket{io, asio::ip::make_address_v4("239.255.0.7"), 7447, 4};
    plexus::discovery::multicast_discovery<plexus::asio::udp_multicast_socket, plexus::asio::asio_policy>
        disc{io, mc_socket};

    plexus::node_options opts;
    opts.name      = "uppercase-server";
    opts.reconnect = plexus::io::reconnect_config{
        std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0x5E54E;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "uppercase-server", transport, opts};
    node.listen({"tcp", "0.0.0.0:5572"});

    using bytes_procedure = plexus::procedure<>;
    bytes_procedure uppercase{node, "uppercase",
                              [](std::span<const std::byte> param, bytes_procedure::reply_fn &reply)
                              {
                                  std::vector<std::byte> out(param.begin(), param.end());
                                  for(auto &b : out)
                                      b = static_cast<std::byte>(
                                          std::toupper(std::to_integer<int>(b)));
                                  reply(plexus::wire::rpc_status::success, out);
                              }};

    io.run();
}
