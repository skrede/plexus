#ifndef HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_INIT_LIST_FIXTURE_H
#define HPP_GUARD_TESTS_SIZE_GATE_FUNCTION_SCREEN_BACKENDS_PLEXUS_DEMO_INIT_LIST_FIXTURE_H

#include <cstdint>

namespace plexus::demo {

class init_list_fixture
{
public:
    explicit init_list_fixture(std::uint32_t seed)
            : m_a(seed)
            , m_b(seed)
            , m_c(seed)
            , m_d(seed)
            , m_e(seed)
            , m_f(seed)
            , m_g(seed)
            , m_h(seed)
            , m_i(seed)
            , m_j(seed)
            , m_k(seed)
            , m_l(seed)
            , m_m(seed)
            , m_n(seed)
            , m_o(seed)
            , m_p(seed)
            , m_q(seed)
            , m_r(seed)
            , m_s(seed)
            , m_t(seed)
            , m_u(seed)
    {
        m_a += m_u;
        m_u += m_a;
    }

    init_list_fixture(std::uint32_t seed, std::uint32_t step)
            : m_a(seed + step)
            , m_b(seed + step)
            , m_c(seed + step)
            , m_d(seed + step)
            , m_e(seed + step)
            , m_f(seed + step)
            , m_g(seed + step)
            , m_h(seed + step)
            , m_i(seed + step)
            , m_j(seed + step)
            , m_k(seed + step)
            , m_l(seed + step)
            , m_m(seed + step)
            , m_n(seed + step)
            , m_o(seed + step)
            , m_p(seed + step)
            , m_q(seed + step)
            , m_r(seed + step)
            , m_s(seed + step)
            , m_t(seed + step)
            , m_u(seed + step)
    {
        m_a += step;
    }

private:
    std::uint32_t m_a;
    std::uint32_t m_b;
    std::uint32_t m_c;
    std::uint32_t m_d;
    std::uint32_t m_e;
    std::uint32_t m_f;
    std::uint32_t m_g;
    std::uint32_t m_h;
    std::uint32_t m_i;
    std::uint32_t m_j;
    std::uint32_t m_k;
    std::uint32_t m_l;
    std::uint32_t m_m;
    std::uint32_t m_n;
    std::uint32_t m_o;
    std::uint32_t m_p;
    std::uint32_t m_q;
    std::uint32_t m_r;
    std::uint32_t m_s;
    std::uint32_t m_t;
    std::uint32_t m_u;
};

}

#endif
