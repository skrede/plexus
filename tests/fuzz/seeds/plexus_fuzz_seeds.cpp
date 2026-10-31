#include "seed_sink.h"

#include <array>
#include <cstdio>
#include <string>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <filesystem>
#include <string_view>
#include <system_error>

namespace {

struct seed_target
{
    std::string_view name;
    seed_set (&make)();
};

const std::array<seed_target, 17> k_targets{{
        {"fuzz_frame_reassembler", frame_reassembler_seeds},
        {"fuzz_data_frame", data_frame_seeds},
        {"fuzz_varint", varint_seeds},
        {"fuzz_rpc_frame", rpc_frame_seeds},
        {"fuzz_handshake_frame", handshake_frame_seeds},
        {"fuzz_subscribe_frame", subscribe_frame_seeds},
        {"fuzz_fetch_latched_frame", fetch_latched_frame_seeds},
        {"fuzz_heartbeat_frame", heartbeat_frame_seeds},
        {"fuzz_topic_declaration_frame", topic_declaration_frame_seeds},
        {"fuzz_companion_frame", companion_frame_seeds},
        {"fuzz_announcement_frame", announcement_frame_seeds},
        {"fuzz_peer_report_frame", peer_report_frame_seeds},
        {"fuzz_forwarded_frame", forwarded_frame_seeds},
        {"fuzz_reassembler", reassembler_seeds},
        {"fuzz_key_pattern", key_pattern_seeds},
        {"fuzz_ring_attach", ring_attach_seeds},
        {"fuzz_crc_serial", crc_serial_seeds},
}};

std::string seed_name(std::size_t index)
{
    return (index < 10 ? "seed-0" : "seed-") + std::to_string(index);
}

bool write_seed(const std::filesystem::path &file, const seed &bytes)
{
    std::ofstream out{file, std::ios::binary | std::ios::trunc};
    out.write(reinterpret_cast<const char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    out.close();
    return !out.fail();
}

bool emit(const std::filesystem::path &root, const seed_target &target)
{
    const seed_set seeds            = target.make();
    const std::filesystem::path dir = root / target.name;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if(seeds.empty() || ec)
    {
        std::fprintf(stderr, "plexus_fuzz_seeds: no seeds written for %.*s\n", static_cast<int>(target.name.size()), target.name.data());
        return false;
    }
    for(std::size_t i = 0; i < seeds.size(); ++i)
    {
        if(!write_seed(dir / seed_name(i), seeds[i]))
        {
            std::fprintf(stderr, "plexus_fuzz_seeds: cannot write %s\n", (dir / seed_name(i)).string().c_str());
            return false;
        }
    }
    return true;
}

}

int main(int argc, char **argv)
{
    if(argc != 2 || std::string_view{argv[1]}.empty())
    {
        std::fprintf(stderr, "usage: plexus_fuzz_seeds <corpus-root>\n");
        return EXIT_FAILURE;
    }
    for(const auto &target : k_targets)
    {
        if(!emit(argv[1], target))
            return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
