#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_LAUNCH_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_XPROC_LAUNCH_H

#include "plexus/testing/platform.h"

#if defined(_WIN32)
    #include <windows.h>
#else
    #include <spawn.h>
    #include <unistd.h>
#endif

#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <string_view>

#if !defined(_WIN32)
// posix_spawn inherits the caller's environment; the symbol is not reliably
// declared by <unistd.h> under strict feature-test macros, so declare it here.
extern char **environ;
#endif

namespace plexus::testing {

// argv[1] on a re-exec'd child; absent on the ordinary Catch2 run.
inline constexpr std::string_view k_xproc_role_marker = "--plexus-xproc-child";

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

inline std::wstring xproc_command_line(const std::wstring &exe, std::string_view key, const std::vector<std::string> &args)
{
    std::wstring cmd = xproc_quote(exe);
    cmd += L' ';
    cmd += xproc_widen(k_xproc_role_marker);
    cmd += L' ';
    cmd += xproc_quote(xproc_widen(key));
    for(const auto &a : args)
    {
        cmd += L' ';
        cmd += xproc_quote(xproc_widen(a));
    }
    return cmd;
}

using xproc_child_handle = HANDLE;

inline std::optional<xproc_child_handle> xproc_spawn(std::string_view key, const std::vector<std::string> &args)
{
    const std::wstring exe = xproc_self_exe_wide();
    const std::wstring cmd = xproc_command_line(exe, key, args);
    std::vector<wchar_t> mutable_cmd(cmd.begin(), cmd.end());
    mutable_cmd.push_back(L'\0');

    STARTUPINFOW si{};
    si.cb = sizeof(si);
    PROCESS_INFORMATION pi{};
    if(!::CreateProcessW(exe.c_str(), mutable_cmd.data(), nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si, &pi))
        return std::nullopt;
    ::CloseHandle(pi.hThread);
    return pi.hProcess;
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

inline std::vector<std::string> xproc_child_argv(std::string_view key, const std::vector<std::string> &args)
{
    std::vector<std::string> parts;
    parts.reserve(args.size() + 3);
    parts.emplace_back(xproc_self_exe());
    parts.emplace_back(k_xproc_role_marker);
    parts.emplace_back(key);
    for(const auto &a : args)
        parts.emplace_back(a);
    return parts;
}

using xproc_child_handle = pid_t;

// posix_spawn rather than fork, so the child is a fresh image that re-opens every shared object by name.
inline std::optional<xproc_child_handle> xproc_spawn(std::string_view key, const std::vector<std::string> &args)
{
    std::vector<std::string> parts = xproc_child_argv(key, args);
    std::vector<char *> argv;
    argv.reserve(parts.size() + 1);
    for(auto &p : parts)
        argv.push_back(p.data());
    argv.push_back(nullptr);

    pid_t pid = 0;
    if(::posix_spawn(&pid, parts.front().c_str(), nullptr, nullptr, argv.data(), environ) != 0)
        return std::nullopt;
    return pid;
}

#endif

}

}

#endif
