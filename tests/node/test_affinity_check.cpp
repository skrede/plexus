#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <filesystem>

namespace {

struct child_result
{
    int status;
    std::string out;
    std::string err;
};

std::string read_file(const std::filesystem::path &path)
{
    std::ifstream in{path};
    return std::string{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
}

// cmd.exe strips the outermost pair of quotes from a command that holds more than two, so Windows wraps it once more.
child_result run_child(const std::string &role)
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

void require_trip(const std::string &role, const std::string &member)
{
    const child_result result = run_child(role);
    INFO("stdout: " << result.out << "\nstderr: " << result.err);
    CHECK(result.status != 0);
    CHECK(result.err.find("plexus: " + member + " called off the node's executor thread") != std::string::npos);
}

void require_ok(const std::string &role)
{
    const child_result result = run_child(role);
    INFO("stdout: " << result.out << "\nstderr: " << result.err);
    CHECK(result.status == 0);
    CHECK(result.out.find("affinity-ok") != std::string::npos);
    CHECK(result.err.find("called off the node's executor thread") == std::string::npos);
}

}

TEST_CASE("affinity_check foreign_publish aborts with a message naming the member", "[node]")
{
    require_trip("foreign_publish", "publish");
}

TEST_CASE("affinity_check foreign_member aborts with a message naming the member", "[node]")
{
    require_trip("foreign_member", "participants");
}

TEST_CASE("affinity_check second_runner aborts with a message naming the member", "[node]")
{
    require_trip("second_runner", "participants");
}

TEST_CASE("affinity_check foreign_handle_destroy aborts with a message naming the member", "[node]")
{
    require_trip("foreign_handle_destroy", "retire_subscriber");
}

TEST_CASE("affinity_check inproc_foreign aborts with a message naming the member", "[node]")
{
    require_trip("inproc_foreign", "participants");
}

TEST_CASE("affinity_check via_post the same calls through plexus::post do not trip", "[node]")
{
    require_ok("via_post");
}

TEST_CASE("affinity_check foreign_release aborts with a message naming the member", "[node]")
{
    require_trip("foreign_release", "release_driver");
}

TEST_CASE("affinity_check asio_teardown_unreleased aborts when the owner tears down a handle the joined driver still claims", "[node]")
{
    require_trip("asio_teardown_unreleased", "retire_subscriber");
}

TEST_CASE("affinity_check asio_release_teardown lets the owner tear down a handle after the driver released and was joined", "[node]")
{
    require_ok("asio_release_teardown");
}

TEST_CASE("affinity_check inproc_teardown_unreleased aborts when the owner tears down a handle the joined driver still claims", "[node]")
{
    require_trip("inproc_teardown_unreleased", "retire_subscriber");
}

TEST_CASE("affinity_check inproc_release_teardown lets the owner tear down a handle after the driver released and was joined", "[node]")
{
    require_ok("inproc_release_teardown");
}

TEST_CASE("affinity_check freertos_release_teardown lets the owner tear down a handle after the driver released and was joined", "[node]")
{
    require_ok("freertos_release_teardown");
}

TEST_CASE("affinity_check inproc_pump_handover lets a thread that drains the executor claim it from the setup thread", "[node][!shouldfail]")
{
    require_ok("inproc_pump_handover");
}

TEST_CASE("affinity_check asio_poll_handover lets a thread that polls the io_context claim it from the setup thread", "[node][!shouldfail]")
{
    require_ok("asio_poll_handover");
}

TEST_CASE("affinity_check inproc_second_stepper aborts when a second thread steps an executor the first still claims", "[node][!shouldfail]")
{
    require_trip("inproc_second_stepper", "step");
}

TEST_CASE("affinity_check freertos_pump_handover lets a task that pumps the executor claim it from the setup task", "[node][!shouldfail]")
{
    require_ok("freertos_pump_handover");
}

TEST_CASE("affinity_check freertos_second_pumper aborts when a second task pumps an executor the first still claims", "[node][!shouldfail]")
{
    require_trip("freertos_second_pumper", "pump");
}
