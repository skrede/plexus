#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H

#include "xproc_launch.h"

#if !defined(_WIN32)
    #include <sys/mman.h>
#endif

#include <map>
#include <string>
#include <vector>
#include <utility>
#include <string_view>

// The cross-process test substrate every SHM xproc case reuses. It launches a
// child by RE-EXECUTING the current test binary with an argv role marker, rather
// than fork()+copy-on-write inheritance (which Windows has no analog for). The
// child re-opens the shared region and notifier BY NAME -- the exact open-by-name
// path a real cross-process consumer takes, which fork's COW inheritance silently
// bypasses. The child routine is REGISTERED under a string key (a fresh process
// image cannot receive a C++ closure across exec); the parent passes that key plus
// the shared-object name(s) as argv. The child's boolean becomes its process exit
// status; the parent predicate runs STRICTLY AFTER the child exits, so writes the
// child committed to the shared objects are visible. Header-only; depends only on
// the portable test helper and the shared-object names, never on a plexus target.

namespace plexus::testing {

// A registered child routine: it receives the argv-passed names (the shared
// objects to re-open) and returns success. The boolean becomes the child process
// exit status the parent reads back as child_succeeded.
using xproc_child_fn = bool (*)(const std::vector<std::string> &args);

inline std::map<std::string, xproc_child_fn> &xproc_child_registry()
{
    static std::map<std::string, xproc_child_fn> registry;
    return registry;
}

// Register a child routine under a key at static-init time (before Catch2 runs).
// Returns a bool so a translation unit can register via a namespace-scope const.
inline bool register_xproc_child(std::string key, xproc_child_fn fn)
{
    xproc_child_registry()[std::move(key)] = fn;
    return true;
}

// The outcome of a cross-process round-trip: did the child routine return true,
// and did the parent predicate (run after the child exited) return true.
struct xproc_outcome
{
    bool child_succeeded  = false;
    bool parent_succeeded = false;

    bool ok() const noexcept
    {
        return child_succeeded && parent_succeeded;
    }
};

// Launch the child routine registered under `child_key`, passing `child_args` (the
// shared-object names it must re-open). The parent predicate runs strictly after
// the child has exited, so the child's committed shared writes are visible. A
// launch failure leaves both outcomes false and skips the parent predicate.
template<typename ParentFn>
xproc_outcome run_xproc(std::string_view child_key, const std::vector<std::string> &child_args, ParentFn &&parent)
{
    xproc_outcome outcome;
    const int status = detail::xproc_launch_and_wait(child_key, child_args);
    if(status < 0)
        return outcome;

    outcome.child_succeeded  = status == 0;
    outcome.parent_succeeded = static_cast<bool>(parent());
    return outcome;
}

// A scoped named shared-region token whose name is unique to the calling process
// (so concurrent ctest shards never collide). On POSIX it shm_unlink()s the name
// on construction and destruction so a crashed case never orphans a /dev/shm
// region; on Windows named mappings/events are refcounted and auto-freed when the
// last handle closes, so the removal is a no-op there.
class scoped_shm_name
{
public:
    explicit scoped_shm_name(std::string_view prefix)
            : m_name(std::string(prefix) + std::to_string(process_id()))
    {
        remove();
    }

    ~scoped_shm_name()
    {
        remove();
    }

    scoped_shm_name(const scoped_shm_name &)            = delete;
    scoped_shm_name &operator=(const scoped_shm_name &) = delete;

    const char *c_str() const noexcept
    {
        return m_name.c_str();
    }
    const std::string &str() const noexcept
    {
        return m_name;
    }

private:
    void remove() noexcept
    {
#if !defined(_WIN32)
        ::shm_unlink(m_name.c_str());
#endif
    }

    std::string m_name;
};

}

#endif
