#include "fuzz_sink.h"
#include "fuzz_region.h"
#include "fuzz_ring_driver.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace {

using namespace plexus::shm;
using plexus::node_id;
using lifetime = plexus::shm::detail::ring_lifetime;

constexpr std::size_t k_max_control_bytes = 64 * 1024;
constexpr std::size_t k_max_slab_bytes    = 1024 * 1024;
constexpr node_id k_publisher{std::byte{0x5a}, std::byte{0xa5}};

void attach_and_drive(fuzz_input &in, fuzz_region &control, fuzz_region &slab, node_id publisher, bool flip)
{
    if(flip)
        publisher[0] ^= std::byte{1};
    fuzz_consume(lifetime::magic_of(control.bytes()));
    broadcast_ring ring;
    const loan_status attached = broadcast_ring::attach(control.bytes(), slab.bytes(), publisher, ring);
    fuzz_consume(attached);
    if(attached != loan_status::ok)
        return;
    fuzz_ring::ring_driver driver{ring, control, in};
    driver.run();
}

// The control region takes the size the image records, as the slab does, so a consistent header is reachable.
void fuzz_raw(fuzz_input &in, std::uint8_t mode)
{
    const auto image         = in.take(static_cast<std::size_t>(in.uint(2)));
    const auto control_bytes = image_field<std::uint64_t>(image, offsetof(control_header_t, control_bytes));
    const auto slab_bytes    = image_field<std::uint64_t>(image, offsetof(control_header_t, slab_bytes));
    if(slab_bytes > k_max_slab_bytes)
        return;
    fuzz_region control{image, static_cast<std::size_t>(control_bytes <= k_max_control_bytes ? control_bytes : image.size())};
    fuzz_region slab{static_cast<std::size_t>(slab_bytes)};
    attach_and_drive(in, control, slab, image_field<node_id>(image, offsetof(control_header_t, publisher)), (mode & 2) != 0);
}

void fuzz_mutated(fuzz_input &in, std::uint8_t mode)
{
    const std::uint64_t cells    = std::uint64_t{1} << (in.u8() % 6);
    const std::uint64_t slot     = 1 + in.u8() % 64;
    const std::uint64_t capacity = 1 + in.u8() % k_max_consumers;
    fuzz_region control{control_region_bytes(cells)};
    fuzz_region slab{slab_region_bytes(cells, slot)};
    broadcast_ring creator;
    if(broadcast_ring::create(control.bytes(), slab.bytes(), cells, slot, k_publisher, creator, capacity) != loan_status::ok)
        std::abort();
    control.tamper(in, in.u8() % 32);
    if((mode & 4) != 0)
        creator.header().magic.store(k_ring_retired, std::memory_order_release);
    attach_and_drive(in, control, slab, k_publisher, (mode & 2) != 0);
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_input in{std::span<const std::uint8_t>{data, size}};
    const std::uint8_t mode = in.u8();
    if((mode & 1) != 0)
        fuzz_raw(in, mode);
    else
        fuzz_mutated(in, mode);
    return 0;
}
