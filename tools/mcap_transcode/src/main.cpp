#include "plexus/tools/flat_to_mcap.h"

#include <span>
#include <vector>
#include <cstddef>
#include <fstream>
#include <iostream>
#include <iterator>
#include <filesystem>

namespace {

std::vector<std::byte> slurp(const std::filesystem::path &path, bool &ok)
{
    std::ifstream in{path, std::ios::binary};
    if(!in)
    {
        ok = false;
        return {};
    }
    std::vector<char> raw{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    std::vector<std::byte> bytes(raw.size());
    for(std::size_t i = 0; i < raw.size(); ++i)
        bytes[i] = static_cast<std::byte>(raw[i]);
    ok = true;
    return bytes;
}

}

int main(int argc, char **argv)
{
    if(argc != 3)
    {
        std::cerr << "usage: " << (argc > 0 ? argv[0] : "mcap_transcode")
                  << " <capture.plxr> <out.mcap>\n";
        return 2;
    }

    bool       read_ok = false;
    const auto flat    = slurp(argv[1], read_ok);
    if(!read_ok)
    {
        std::cerr << "error: cannot read flat capture: " << argv[1] << '\n';
        return 1;
    }

    const auto result = plexus::tools::flat_to_mcap(flat, argv[2]);
    if(!result.ok)
    {
        std::cerr << "error: " << result.error << '\n';
        return 1;
    }

    std::cout << "wrote " << argv[2] << ": " << result.messages << " messages across "
              << result.channels << " channels (" << result.schemas << " schemas); recovered "
              << result.recovered << " records";
    if(result.trailing_partial_dropped)
        std::cout << ", trailing partial dropped";
    if(result.corruption_skipped)
        std::cout << ", corruption skipped";
    std::cout << '\n';
    return 0;
}
