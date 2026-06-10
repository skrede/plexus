#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include <span>
#include <cstddef>

// Consumes the INSTALLED package through plexus::plexus only. Constructing both
// vocabulary types proves target resolution, the include paths, and the
// core->wire transitive link all resolve from the installed tree.
int main()
{
    plexus::node_id id{};
    id[0] = std::byte{0x42};

    std::byte storage[4]{};
    plexus::wire_bytes<> bytes{std::span<const std::byte>{storage}, nullptr};

    return bytes.size() == sizeof(storage) ? 0 : 1;
}
