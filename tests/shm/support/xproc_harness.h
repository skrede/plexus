#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H

#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <string_view>

// The cross-process (fork) test substrate every SHM xproc case reuses
// (lost-wakeup, same-host-roundtrip, teardown-race, stale-reclaim). It forks,
// runs a child predicate in the child (whose boolean result becomes the child's
// exit status), waitpid()s the child to completion, then runs a parent
// predicate AFTER the child has exited so the parent observes the child's
// committed shared-memory writes without a fork-vs-write race. It also offers a
// scoped /dev/shm region name that shm_unlink()s on teardown — test hygiene so a
// crashed case never orphans a named region (T-19-01). Header-only; depends only
// on POSIX, never on a plexus target.

namespace plexus::testing {

// The outcome of a forked round-trip: did the child predicate return true, and
// did the parent predicate (run after the child exited) return true.
struct xproc_outcome
{
    bool child_succeeded  = false;
    bool parent_succeeded = false;

    bool ok() const noexcept
    {
        return child_succeeded && parent_succeeded;
    }
};

// Fork; run `child` in the child (its bool result is the child exit status);
// waitpid the child; then run `parent` in the parent. The parent runs strictly
// after the child has exited, so shared-region writes the child committed are
// guaranteed visible. A parent predicate that wants to verify only post-exit
// state is the common shape; pass an always-true parent to assert child-only.
template<typename ChildFn, typename ParentFn>
xproc_outcome run_forked(ParentFn &&parent, ChildFn &&child)
{
    const pid_t pid = ::fork();
    if(pid == 0)
    {
        const bool ok = static_cast<bool>(child());
        ::_exit(ok ? 0 : 1);
    }

    xproc_outcome outcome;
    if(pid < 0)
        return outcome;

    int status = 0;
    while(::waitpid(pid, &status, 0) < 0 && errno == EINTR)
        ;
    outcome.child_succeeded  = WIFEXITED(status) && WEXITSTATUS(status) == 0;
    outcome.parent_succeeded = static_cast<bool>(parent());
    return outcome;
}

// A scoped named /dev/shm region: shm_unlink()s its name on destruction so no
// test leaves a region behind even on an assertion failure (T-19-01). The name
// is unique to the calling process to avoid collisions across concurrent ctest
// shards.
class scoped_shm_name
{
public:
    explicit scoped_shm_name(std::string_view prefix)
            : m_name(std::string(prefix) + std::to_string(::getpid()))
    {
        ::shm_unlink(m_name.c_str());
    }

    ~scoped_shm_name()
    {
        ::shm_unlink(m_name.c_str());
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
    std::string m_name;
};

}

#endif
