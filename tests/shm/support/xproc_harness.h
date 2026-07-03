#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_HARNESS_H

#include "plexus/testing/platform.h"

#if defined(_WIN32)
    #include <windows.h>
#else
    #include <cerrno>
    #include <spawn.h>
    #include <unistd.h>

    #include <sys/mman.h>
    #include <sys/wait.h>
#endif

#include <map>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>
#include <string_view>

#if !defined(_WIN32)
// posix_spawn inherits the caller's environment; the symbol is not reliably
// declared by <unistd.h> under strict feature-test macros, so declare it here.
extern char **environ;
#endif

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

// argv[1] on a re-exec'd child; absent on the ordinary Catch2 run.
inline constexpr std::string_view k_xproc_role_marker = "--plexus-xproc-child";

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

// The test's main() records argv here so the parent can re-exec the same binary
// (argv[0] is the fallback when the platform self-path lookup is unavailable).
inline std::vector<std::string> &xproc_self_argv()
{
    static std::vector<std::string> argv;
    return argv;
}

inline void xproc_capture_argv(int argc, char **argv)
{
    auto &store = xproc_self_argv();
    store.clear();
    store.reserve(static_cast<std::size_t>(argc));
    for(int i = 0; i < argc; ++i)
        store.emplace_back(argv[i]);
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

namespace detail {

#if defined(_WIN32)

inline std::wstring xproc_widen(std::string_view s)
{
    if(s.empty())
        return std::wstring();
    const int n = ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), nullptr, 0);
    std::wstring out(static_cast<std::size_t>(n), L'\0');
    ::MultiByteToWideChar(CP_UTF8, 0, s.data(), static_cast<int>(s.size()), out.data(), n);
    return out;
}

inline std::wstring xproc_self_exe_wide()
{
    std::wstring buf(32768, L'\0');
    const DWORD n = ::GetModuleFileNameW(nullptr, buf.data(), static_cast<DWORD>(buf.size()));
    buf.resize(n);
    return buf;
}

inline std::wstring xproc_quote(const std::wstring &s)
{
    std::wstring out;
    out.push_back(L'"');
    for(const wchar_t c : s)
    {
        if(c == L'"')
            out.push_back(L'\\');
        out.push_back(c);
    }
    out.push_back(L'"');
    return out;
}

// Re-exec self as a child with the role marker + key + names; wait on process
// exit and return the child exit code (a negative value flags a launch failure).
inline int xproc_launch_and_wait(std::string_view key, const std::vector<std::string> &args)
{
    const std::wstring exe = xproc_self_exe_wide();
    std::wstring cmd       = xproc_quote(exe);
    cmd += L' ';
    cmd += xproc_widen(k_xproc_role_marker);
    cmd += L' ';
    cmd += xproc_quote(xproc_widen(key));
    for(const auto &a : args)
    {
        cmd += L' ';
        cmd += xproc_quote(xproc_widen(a));
    }

    std::vector<wchar_t> mutable_cmd(cmd.begin(), cmd.end());
    mutable_cmd.push_back(L'\0');

    STARTUPINFOW si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};
    if(!::CreateProcessW(exe.c_str(), mutable_cmd.data(), nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi))
        return -1;

    ::WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD code = 1;
    ::GetExitCodeProcess(pi.hProcess, &code);
    ::CloseHandle(pi.hProcess);
    ::CloseHandle(pi.hThread);
    return static_cast<int>(code);
}

#else

inline std::string xproc_self_exe()
{
    char buf[4096];
    const ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if(n > 0)
        return std::string(buf, static_cast<std::size_t>(n));
    const auto &argv = xproc_self_argv();
    return argv.empty() ? std::string() : argv.front();
}

// posix_spawn the current binary (NOT fork) with the role marker + key + names;
// waitpid the child through EINTR and return its exit code (negative on failure).
inline int xproc_launch_and_wait(std::string_view key, const std::vector<std::string> &args)
{
    std::vector<std::string> parts;
    parts.reserve(args.size() + 3);
    parts.emplace_back(xproc_self_exe());
    parts.emplace_back(k_xproc_role_marker);
    parts.emplace_back(key);
    for(const auto &a : args)
        parts.emplace_back(a);

    std::vector<char *> argv;
    argv.reserve(parts.size() + 1);
    for(auto &p : parts)
        argv.push_back(p.data());
    argv.push_back(nullptr);

    pid_t pid          = 0;
    const int spawn_rc = ::posix_spawn(&pid, parts.front().c_str(), nullptr, nullptr, argv.data(), environ);
    if(spawn_rc != 0)
        return -1;

    int status = 0;
    while(::waitpid(pid, &status, 0) < 0 && errno == EINTR)
        ;
    if(WIFEXITED(status))
        return WEXITSTATUS(status);
    return -1;
}

#endif

}

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
