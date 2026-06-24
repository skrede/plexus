#include "plexus/native/shm_backend_version.h"

namespace plexus::native {

std::string_view backend_version() noexcept
{
    return "0.1.0";
}

}
