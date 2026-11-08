#include "plexus/freertos/run_task.h"
#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/null_stream_socket.h"

#include "plexus/io/endpoint.h"

#include <cstdio>
#include <memory>
#include <csignal>
#include <cstdlib>
#include <optional>
#include <exception>
#include <string_view>
#include <type_traits>

#if !defined(_WIN32)
    #include <sys/resource.h>
#endif

namespace {

using null_channel = plexus::freertos::lwip_channel<plexus::freertos::detail::null_stream_socket>;

static_assert(std::is_nothrow_destructible_v<plexus::freertos::freertos_executor>);

// _Exit, so no later destructor or leak check can produce the exit status the test expects.
void exit_without_abort(const char *what)
{
    std::fprintf(stderr, "no abort: %s\n", what);
    std::_Exit(EXIT_SUCCESS);
}

void exit_on_escaped_exception()
{
    exit_without_abort("an exception escaped into std::terminate");
}

void exit_on_abort(int)
{
    std::_Exit(EXIT_FAILURE);
}

void destroy_executor_under_live_task()
{
    std::optional<plexus::freertos::freertos_executor> ex{std::in_place};
    null_channel channel(plexus::freertos::detail::null_stream_socket{}, *ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"});
    if(plexus::freertos::spawn_lwip_rx_task(channel, *ex, plexus::freertos::task_options{4096}) != pdTRUE)
        exit_without_abort("the receive task was not spawned");
    ex.reset();
    exit_without_abort("the executor was destroyed under a live task");
}

// The executor outlives the channel; _Exit ends the program before it is destroyed under the still-counted task.
void destroy_channel_under_live_task()
{
    plexus::freertos::freertos_executor ex;
    auto channel = std::make_unique<null_channel>(plexus::freertos::detail::null_stream_socket{}, ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"});
    if(plexus::freertos::spawn_lwip_rx_task(*channel, ex, plexus::freertos::task_options{4096}) != pdTRUE)
        exit_without_abort("the receive task was not spawned");
    channel.reset();
    exit_without_abort("the channel was destroyed under a live receive task");
}

}

// WILL_FAIL does not invert a signal, so the expected abort exits 1 and every other outcome exits 0. An expected abort
// leaves no core file or abort dialog behind.
int main(int argc, char **argv)
{
#if defined(_MSC_VER)
    _set_abort_behavior(0, _WRITE_ABORT_MSG | _CALL_REPORTFAULT);
#elif !defined(_WIN32)
    const rlimit no_core{0, 0};
    setrlimit(RLIMIT_CORE, &no_core);
#endif
    std::set_terminate(&exit_on_escaped_exception);
    std::signal(SIGABRT, &exit_on_abort);
    if(argc > 1 && std::string_view{argv[1]} == "executor")
        destroy_executor_under_live_task();
    if(argc > 1 && std::string_view{argv[1]} == "channel")
        destroy_channel_under_live_task();
    return EXIT_SUCCESS;
}
