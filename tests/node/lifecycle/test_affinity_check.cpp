#include "affinity_child_runner.h"

#include <catch2/catch_test_macros.hpp>

using plexus::testing::require_ok;
using plexus::testing::require_trip;
using plexus::testing::require_abort;

TEST_CASE("affinity_check foreign_publish aborts with a message naming the member", "[node]")
{
    require_trip("foreign_publish", "publish");
}

TEST_CASE("affinity_check moved_from_publish aborts with a message naming the member", "[node]")
{
    require_abort("moved_from_publish", "plexus: publish called on a moved-from handle");
}

TEST_CASE("affinity_check moved_from_bytes_call aborts with a message naming the member", "[node]")
{
    require_abort("moved_from_bytes_call", "plexus: call called on a moved-from handle");
}

TEST_CASE("affinity_check moved_from_typed_call aborts with a message naming the member", "[node]")
{
    require_abort("moved_from_typed_call", "plexus: call called on a moved-from handle");
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

TEST_CASE("affinity_check inproc_pump_handover lets a thread that drains the executor claim it from the setup thread", "[node]")
{
    require_ok("inproc_pump_handover");
}

TEST_CASE("affinity_check asio_poll_handover lets a thread that polls the io_context claim it from the setup thread", "[node]")
{
    require_ok("asio_poll_handover");
}

TEST_CASE("affinity_check inproc_second_stepper aborts when a second thread steps an executor the first still claims", "[node]")
{
    require_trip("inproc_second_stepper", "step");
}

TEST_CASE("affinity_check freertos_pump_handover lets a task that pumps the executor claim it from the setup task", "[node]")
{
    require_ok("freertos_pump_handover");
}

TEST_CASE("affinity_check freertos_second_pumper aborts when a second task pumps an executor the first still claims", "[node]")
{
    require_trip("freertos_second_pumper", "pump");
}

TEST_CASE("affinity_check foreign_recorder_pump aborts with a message naming the member", "[node]")
{
    require_trip("foreign_recorder_pump", "recorder::pump");
}

TEST_CASE("affinity_check foreign_recorder_flush aborts with a message naming the member", "[node]")
{
    require_trip("foreign_recorder_flush", "recorder::flush");
}

TEST_CASE("affinity_check foreign_recorder_trigger aborts with a message naming the member", "[node]")
{
    require_trip("foreign_recorder_trigger", "recorder::trigger");
}

TEST_CASE("affinity_check foreign_recorder_captured_span aborts with a message naming the member", "[node]")
{
    require_trip("foreign_recorder_captured_span", "recorder::captured_span");
}

TEST_CASE("affinity_check foreign_recorder_preamble aborts with a message naming the member", "[node]")
{
    require_trip("foreign_recorder_preamble", "recorder::preamble");
}

TEST_CASE("affinity_check foreign_subscriber_decode_failed aborts with a message naming the member", "[node]")
{
    require_trip("foreign_subscriber_decode_failed", "decode_failed");
}

TEST_CASE("affinity_check foreign_value_logger_decode_failed aborts with a message naming the member", "[node]")
{
    require_trip("foreign_value_logger_decode_failed", "decode_failed");
}
