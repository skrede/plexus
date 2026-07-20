#ifndef HPP_GUARD_PLEXUS_MATCH_DETAIL_MATCH_ENGINE_H
#define HPP_GUARD_PLEXUS_MATCH_DETAIL_MATCH_ENGINE_H

#include "plexus/match/detail/match_tokens.h"
#include "plexus/match/detail/segment_cursor.h"

#include <array>
#include <cstddef>

namespace plexus::match::detail
{

constexpr std::size_t k_no_star = static_cast<std::size_t>(-1);

template<typename Pattern>
constexpr bool is_concrete(const Pattern &pattern) noexcept
{
    for(std::size_t i = 0; i < pattern.segment_count(); ++i)
        if(pattern.segment(i).kind != segment_kind::literal)
            return false;
    return true;
}

struct greedy_state
{
    std::size_t key_i;
    std::size_t pat_j;
    std::size_t star_key;
    std::size_t star_pat;
};

// One transition of the iterative "one saved star" wildcard match: consume a
// matching segment, open a '**' run, or backtrack the last opened '**'. Returns
// false only when no move is possible (the pattern rejects the key).
// Source: en.wikipedia.org/wiki/Matching_wildcards (the linear, no-backtracking-blowup algorithm).
template<typename Pattern, typename Key>
constexpr bool greedy_step(const Pattern &pattern, const Key &key, greedy_state &s) noexcept
{
    const std::size_t n = pattern.segment_count();
    if(s.pat_j < n && single_absorbs(pattern.segment(s.pat_j), key.segment(s.key_i)))
    {
        ++s.key_i;
        ++s.pat_j;
    }
    else if(s.pat_j < n && pattern.segment(s.pat_j).kind == segment_kind::double_star)
    {
        s.star_pat = s.pat_j;
        s.star_key = s.key_i;
        ++s.pat_j;
    }
    else if(s.star_pat != k_no_star)
    {
        s.key_i = ++s.star_key;
        s.pat_j = s.star_pat + 1;
    }
    else
        return false;
    return true;
}

// Greedy two-pointer match of a wildcard pattern against a concrete key:
// O(#key segments) time, O(1) extra state, no recursion (the query/universe hot path).
template<typename Pattern, typename Key>
constexpr bool matches_concrete(const Pattern &pattern, const Key &key) noexcept
{
    greedy_state s{0, 0, 0, k_no_star};
    while(s.key_i < key.segment_count())
        if(!greedy_step(pattern, key, s))
            return false;
    while(s.pat_j < pattern.segment_count() && pattern.segment(s.pat_j).kind == segment_kind::double_star)
        ++s.pat_j;
    return s.pat_j == pattern.segment_count();
}

// One intersection cell: with '**' on either side the run absorbs a segment
// (right) or matches empty (below); otherwise the two single tokens must be
// compatible and their tails intersect (diag).
constexpr bool intersect_cell(segment_view a, segment_view b, bool right, bool below, bool diag) noexcept
{
    if(a.kind == segment_kind::double_star || b.kind == segment_kind::double_star)
        return right || below;
    return tokens_compatible(a, b) && diag;
}

// One containment cell: an a-'**' absorbs a run of b (right) or matches empty
// (below); otherwise the single a token must cover the single b token (never a
// b-'**') and their tails contain (diag). Only a-side '**' drives the absorb.
constexpr bool include_cell(segment_view a, segment_view b, bool right, bool below, bool diag) noexcept
{
    if(a.kind == segment_kind::double_star)
        return right || below;
    return covers_single(a, b) && diag;
}

template<typename Cell, typename Pattern, typename Row>
constexpr void roll_column(Cell cell, const Pattern &a, const Pattern &b, std::size_t i, Row &row) noexcept
{
    const std::size_t  db    = b.segment_count();
    const segment_view aseg  = a.segment(i);
    const bool         astar = aseg.kind == segment_kind::double_star;
    bool               diag  = row[db];
    row[db]                  = astar && row[db];
    for(std::size_t j = db; j-- > 0;)
    {
        const bool below = row[j];
        row[j]           = cell(aseg, b.segment(j), row[j + 1], below, diag);
        diag             = below;
    }
}

// b-exhausted boundary shared by both relations: a[i:] denotes the empty key only
// when every remaining a token is '**', so the last column keeps that suffix flag.
template<typename Pattern, typename Row>
constexpr void seed_intersect_row(const Pattern &b, Row &row) noexcept
{
    const std::size_t db = b.segment_count();
    row[db]              = true;
    for(std::size_t j = db; j-- > 0;)
        row[j] = (b.segment(j).kind == segment_kind::double_star) && row[j + 1];
}

// Rolling-row DP over segment tokens (rolled to one std::array<bool, depth+1> row).
// O(Da*Db) time, O(depth) working set, no recursion. intersects seeds the
// all-'**' b-suffix (a exhausted may still cover an all-'**' b tail); includes
// seeds only the empty-key cell (a exhausted covers b only when b is exhausted).
template<typename Bounds, typename Pattern>
constexpr bool intersects_dp(const Pattern &a, const Pattern &b) noexcept
{
    std::array<bool, Bounds::k_pattern_depth + 1> row{};
    seed_intersect_row(b, row);
    for(std::size_t i = a.segment_count(); i-- > 0;)
        roll_column(intersect_cell, a, b, i, row);
    return row[0];
}

template<typename Bounds, typename Pattern>
constexpr bool includes_dp(const Pattern &a, const Pattern &b) noexcept
{
    std::array<bool, Bounds::k_pattern_depth + 1> row{};
    row[b.segment_count()] = true;
    for(std::size_t i = a.segment_count(); i-- > 0;)
        roll_column(include_cell, a, b, i, row);
    return row[0];
}

}

#endif
