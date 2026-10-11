#include "support/xproc_launch.h"
#include "support/xproc_child_main.h"

#include <catch2/catch_session.hpp>

int main(int argc, char **argv)
{
    plexus::testing::xproc_capture_argv(argc, argv);
    plexus::testing::xproc_child_main(argc, argv);
    return Catch::Session().run(argc, argv);
}
