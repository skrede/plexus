#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_FD_EXHAUSTION_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_FD_EXHAUSTION_H

#if !defined(_WIN32)

    #include <fcntl.h>
    #include <unistd.h>
    #include <sys/resource.h>

    #include <cerrno>
    #include <vector>
    #include <cstdlib>
    #include <algorithm>
    #include <filesystem>

namespace plexus::testing {

// Fills the descriptor table until the next allocation fails with EMFILE, and frees it again on release. The soft
// RLIMIT_NOFILE drops to just above the highest open descriptor first, so the fill takes a handful of opens rather
// than the whole hard limit; whatever the case needs must already be open.
class fd_exhaustion
{
public:
    fd_exhaustion()
            : m_saved(current_limit())
            , m_fillers()
            , m_full(false)
    {
        lower_limit(highest_open_fd() + 1 + margin);
        for(int fd = ::open("/dev/null", O_RDONLY); fd >= 0; fd = ::open("/dev/null", O_RDONLY))
            m_fillers.push_back(fd);
        m_full = errno == EMFILE;
    }

    fd_exhaustion(const fd_exhaustion &)            = delete;
    fd_exhaustion &operator=(const fd_exhaustion &) = delete;

    ~fd_exhaustion()
    {
        release();
    }

    bool full() const noexcept
    {
        return m_full;
    }

    void release() noexcept
    {
        for(const int fd : m_fillers)
            ::close(fd);
        m_fillers.clear();
        (void)::setrlimit(RLIMIT_NOFILE, &m_saved);
    }

private:
    static constexpr int margin = 8;

    ::rlimit m_saved;
    std::vector<int> m_fillers;
    bool m_full;

    static ::rlimit current_limit() noexcept
    {
        ::rlimit lim{};
        (void)::getrlimit(RLIMIT_NOFILE, &lim);
        return lim;
    }

    static int highest_open_fd()
    {
        int highest = 2;
        for(const auto &entry : std::filesystem::directory_iterator("/dev/fd"))
            highest = std::max(highest, static_cast<int>(std::strtol(entry.path().filename().c_str(), nullptr, 10)));
        return highest;
    }

    void lower_limit(int ceiling) noexcept
    {
        ::rlimit lim = m_saved;
        lim.rlim_cur = std::min(m_saved.rlim_cur, static_cast<::rlim_t>(ceiling));
        (void)::setrlimit(RLIMIT_NOFILE, &lim);
    }
};

}

#endif

#endif
