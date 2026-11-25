#include <cstdint>

namespace plexus::demo {

std::uint32_t crash_overage(std::uint32_t value)
{
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    value += 1;
    return value;
}

}
