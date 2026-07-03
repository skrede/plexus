#include "plexus/native/shm_backend_version.h"

#include <catch2/catch_test_macros.hpp>

#if !defined(_WIN32)
    #include <cerrno>
    #include <unistd.h>

    #include <sys/mman.h>
    #include <sys/wait.h>

    #include <atomic>
    #include <cstdint>
#endif

// Proves two host facts: (1) the gated plexus::native backend links and runs
// (backend_version is a real linked symbol), and (2) a forked child and its
// parent share an anonymous MAP_SHARED page, so a value written by the child is
// observed by the parent across the address-space boundary.
//
// The MAP_SHARED case deliberately uses fork() + copy-on-write inheritance of an
// ANONYMOUS mapping -- there is no name to re-open, so it cannot ride the re-exec
// open-by-name substrate the rest of the SHM xproc suite now uses; it is the one
// case that genuinely needs inherited address space. It is a POSIX-only fork COW
// sanity check with no Windows analog, so it compiles out there.

TEST_CASE("scaffold: the gated shm backend links and runs", "[shm][scaffold]")
{
    REQUIRE(plexus::native::backend_version() == "0.1.0");
}

#if !defined(_WIN32)

TEST_CASE("scaffold: a value round-trips through MAP_SHARED across fork", "[shm][scaffold]")
{
    constexpr int k_iterations = 128;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto *word = static_cast<std::atomic<std::uint32_t> *>(::mmap(nullptr, sizeof(std::atomic<std::uint32_t>), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0));
        REQUIRE(word != MAP_FAILED);
        new(word) std::atomic<std::uint32_t>{0};

        const std::uint32_t expected = 0xA5A50000u | static_cast<std::uint32_t>(i);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            word->store(expected, std::memory_order_release);
            ::_exit(0);
        }

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0 && errno == EINTR)
            ;

        const std::uint32_t observed = word->load(std::memory_order_acquire);
        ::munmap(word, sizeof(std::atomic<std::uint32_t>));

        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
        REQUIRE(observed == expected);
    }
}

#endif
