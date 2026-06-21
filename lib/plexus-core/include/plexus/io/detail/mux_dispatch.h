#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_MUX_DISPATCH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_MUX_DISPATCH_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include <array>
#include <tuple>
#include <memory>
#include <cstddef>
#include <utility>
#include <type_traits>
#include <string_view>

namespace plexus::io::detail {

// The per-scheme select + sink-wiring glue for multiplexing_transport, relocated by friendship:
// each helper reaches the mux's member pack / selector / hook / forwarding sinks through the mux
// reference. The mux_candidate / member_prefers_shm types stay namespace-level in the mux header.

// Wrap a member's minted concrete channel into the erased polymorphic_byte_channel.
template<typename C>
std::unique_ptr<polymorphic_byte_channel> mux_wrap(std::unique_ptr<C> ch)
{
    return std::make_unique<polymorphic_byte_channel>(
            std::make_unique<channel_adapter<C>>(std::move(ch)));
}

// Install one member's four completion sinks, each forwarding the wrapped channel / edge up to the
// mux's own erased sink (a no-op until the mux owner installs it). RELOCATION of wire_member.
template<typename Mux, typename M>
// NOLINTNEXTLINE(readability-function-size)
void wire_member(Mux &mux, M &m)
{
    using C = typename M::channel_type;
    m.on_accepted(
            [&mux](std::unique_ptr<C> ch)
            {
                if(mux.m_on_accepted)
                    mux.m_on_accepted(mux_wrap(std::move(ch)));
            });
    m.on_dialed(
            [&mux](std::unique_ptr<C> ch, const endpoint &ep)
            {
                if(mux.m_on_dialed)
                    mux.m_on_dialed(mux_wrap(std::move(ch)), ep);
            });
    m.on_dial_failed(
            [&mux](const endpoint &ep, io_error e)
            {
                if(mux.m_on_dial_failed)
                    mux.m_on_dial_failed(ep, e);
            });
    m.on_error(
            [&mux](io_error e)
            {
                if(mux.m_on_error)
                    mux.m_on_error(e);
            });
}

// Append member I to the candidate array iff it serves ep.scheme within tier (the per-scheme,
// per-tier eligibility filter). The shm_eligible flag is the member type's compile-time property.
template<typename Mux, std::size_t I, typename Candidates>
void mux_consider(const Mux &, const endpoint &ep, transport_kind tier, Candidates &out,
                  std::size_t &count)
{
    using M = typename Mux::template member_type<I>;
    if(M::mux_tier != tier)
        return;
    for(std::string_view scheme : M::mux_schemes)
        if(scheme == ep.scheme)
        {
            out[count++] = mux_candidate{I, Mux::template member_prefers_shm_v<M>};
            return;
        }
}

template<typename Mux, typename Candidates, std::size_t... I>
void mux_collect_candidates(const Mux &mux, const endpoint &ep, transport_kind tier,
                            Candidates &out, std::size_t &count, std::index_sequence<I...>)
{
    (mux_consider<Mux, I>(mux, ep, tier, out, count), ...);
}

}

#endif
