#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_HEALTH_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DISCOVERY_HEALTH_H

namespace plexus::discovery {

// The self-probe verdict of a discovery leaf. not_yet is the transient pre-window state (no self
// echo observed, the window has not elapsed); healthy means the node saw its own multicast echo;
// no_self_echo and bad_interface are the two named silent-failure modes made observable — loopback
// disabled and an unresolved egress selector respectively.
enum class discovery_health
{
    not_yet,
    healthy,
    no_self_echo,
    bad_interface
};

}

#endif
