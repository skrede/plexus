// libFuzzer harness for the key-pattern matcher. The fuzzer input is split into two
// candidate patterns on the first newline (a single-pattern payload exercises the same
// pattern against itself); each is constructed via key_pattern::make() -- construction
// must never crash, a refusal is a valid outcome, not an abort. When both validate the
// harness enforces the set-relation invariants: intersects symmetric, intersects
// reflexive, and includes implies intersects. ASan catches a stack overflow and -timeout
// catches a multi-second match; no global operator-new trap is installed because libFuzzer
// itself allocates -- zero-alloc on the match path is proven by the alloc_counter unit TU.

#include "plexus/match/key_pattern.h"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace
{

using plexus::match::key_pattern;

std::pair<std::string_view, std::string_view> split(const uint8_t *data, std::size_t size)
{
    const std::string_view all(reinterpret_cast<const char *>(data), size);
    const auto sep = all.find('\n');
    if(sep == std::string_view::npos)
        return {all, all};
    return {all.substr(0, sep), all.substr(sep + 1)};
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
    const auto [text_a, text_b] = split(data, size);
    const auto a = key_pattern::make(text_a);
    const auto b = key_pattern::make(text_b);
    if(a && b)
    {
        const bool a_meets_b = a->intersects(*b);
        if(a_meets_b != b->intersects(*a))
            __builtin_trap();
        if(a->includes(*b) && !a_meets_b)
            __builtin_trap();
        if(!a->intersects(*a) || !b->intersects(*b))
            __builtin_trap();
    }
    return 0;
}
