#include "plexus/shm/shm_backend_version.h"

namespace plexus::shm {

std::string_view backend_version() noexcept
{
    return "0.1.0";
}

}
