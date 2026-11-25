#ifndef HPP_GUARD_TESTS_NODE_LIFECYCLE_AFFINITY_CHILD_RUNNER_H
#define HPP_GUARD_TESTS_NODE_LIFECYCLE_AFFINITY_CHILD_RUNNER_H

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <filesystem>

namespace plexus::testing {

struct child_result
{
    int status;
    std::string out;
    std::string err;
};

inline std::string read_file(const std::filesystem::path &path)
{
    std::ifstream in{path};
    return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
}

// cmd.exe strips the outermost pair of quotes from a command that holds more than two, so Windows wraps it once more.
inline child_result run_child(const std::string &role)
{
    const std::filesystem::path dir = plexus::testing::make_temp_dir("plexus-affinity-");
    const std::filesystem::path out = dir / "stdout.txt";
    const std::filesystem::path err = dir / "stderr.txt";
    std::string command             = "\"" + std::string{PLEXUS_AFFINITY_TRIP_CHILD} + "\" " + role + " > \"" + out.string() + "\" 2> \"" + err.string() + "\"";
#if defined(_WIN32)
    command = "\"" + command + "\"";
#endif
    const int status = std::system(command.c_str());
    child_result result{status, read_file(out), read_file(err)};
    std::filesystem::remove_all(dir);
    return result;
}

inline void require_abort(const std::string &role, const std::string &message)
{
    const child_result result = run_child(role);
    INFO("stdout: " << result.out << "\nstderr: " << result.err);
    CHECK(result.status != 0);
    CHECK(result.err.find(message) != std::string::npos);
}

inline void require_trip(const std::string &role, const std::string &member)
{
    require_abort(role, "plexus: " + member + " called off the node's executor thread");
}

inline void require_ok(const std::string &role)
{
    const child_result result = run_child(role);
    INFO("stdout: " << result.out << "\nstderr: " << result.err);
    CHECK(result.status == 0);
    CHECK(result.out.find("affinity-ok") != std::string::npos);
    CHECK(result.err.find("called off the node's executor thread") == std::string::npos);
}

}

#endif
