#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_PEER_CRED_POLICY_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_PEER_CRED_POLICY_H

#include <cstdint>

namespace plexus::io::security {

// The local-peer credentials read at an AF_UNIX accept: the connecting process's user,
// group, and process id. A plain serializable POD (no handles) — the listener extracts it
// from the accepted socket (SO_PEERCRED on Linux, getpeereid on macOS/BSD with pid=0) and
// the injected policy decides over the VALUE, never a raw socket. pid is 0 where the
// platform does not expose it.
struct peer_cred
{
    std::uint32_t uid{0};
    std::uint32_t gid{0};
    std::uint32_t pid{0};
};

// The injectable AF_UNIX peer-credential contract — mechanism, not policy. The listener
// consults a peer_cred_policy at accept to make the admit/refuse decision over the
// extracted peer_cred value (defense-in-depth ABOVE the 0700 socket mode, which is the
// actual default boundary). Mirrors verify_policy exactly: a pure-virtual decide() over a
// fact value + a concrete named default.
//
// decide() is non-throwing and returns the admit verdict. accepts_without_credentials()
// is the platform-portability seam: on Windows AF_UNIX (no peer creds) the listener cannot
// extract a peer_cred, so it consults THIS instead — a non-accept_any policy returns false
// and the listener REFUSES at listen (fail-closed), never silently admitting an
// unidentifiable peer.
class peer_cred_policy
{
public:
    virtual ~peer_cred_policy() = default;

    // True iff this local peer is authorized given its credentials. Non-throwing.
    [[nodiscard]] virtual bool decide(const peer_cred &cred) const noexcept = 0;

    // True iff this policy admits a peer whose credentials cannot be read (Windows AF_UNIX).
    // accept_any returns true; any restrictive policy returns false (fail-closed at listen).
    [[nodiscard]] virtual bool accepts_without_credentials() const noexcept = 0;
};

// The default peer_cred_policy: admit every local peer (the explicit no-credential-check
// choice, the PSK no-auth precedent). The 0700 socket mode is the actual access boundary;
// the credential allowlist is opt-in defense-in-depth, so its absence is a NAMED explicit
// accept_any, never an accidental open door. It also admits a credential-less peer, so a
// Windows AF_UNIX listener with the default policy still accepts.
class accept_any_peer_cred final : public peer_cred_policy
{
public:
    [[nodiscard]] bool decide(const peer_cred &) const noexcept override
    {
        return true;
    }
    [[nodiscard]] bool accepts_without_credentials() const noexcept override
    {
        return true;
    }
};

// The shared default accept_any policy a listener binds when the caller injects none. A
// function-local static (no namespace-scope static singleton object) — one process-wide
// stateless instance the borrowed-by-const& seam points at by default.
inline const peer_cred_policy &shared_accept_any_peer_cred()
{
    static accept_any_peer_cred policy;
    return policy;
}

}

#endif
