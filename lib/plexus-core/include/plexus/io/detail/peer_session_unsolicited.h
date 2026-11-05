#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_UNSOLICITED_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_UNSOLICITED_H

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/peer_session_gate.h"
#include "plexus/io/detail/peer_session_handshake.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::io::detail {

inline constexpr std::chrono::seconds k_unsolicited_resend_interval{1};

// A fixed table, so an unsolicited flood costs a counter and at most one unsubscribe per topic per interval.
template<typename Clock>
class unsolicited_resend
{
    struct mark
    {
        std::uint64_t topic_hash;
        typename Clock::time_point sent_at;
        bool in_use;
    };

public:
    static constexpr std::size_t k_capacity = 8;

    unsolicited_resend() noexcept
            : m_marks()
    {
    }

    bool due(std::uint64_t topic_hash, typename Clock::time_point now) noexcept
    {
        mark &slot = slot_for(topic_hash);
        if(slot.in_use && slot.topic_hash == topic_hash && now - slot.sent_at < k_unsolicited_resend_interval)
            return false;
        slot = mark{topic_hash, now, true};
        return true;
    }

    void clear() noexcept
    {
        m_marks.fill(mark{});
    }

private:
    std::array<mark, k_capacity> m_marks;

    mark &slot_for(std::uint64_t topic_hash) noexcept
    {
        const auto own = std::ranges::find_if(m_marks, [topic_hash](const mark &m) { return m.in_use && m.topic_hash == topic_hash; });
        if(own != m_marks.end())
            return *own;
        const auto idle = std::ranges::find_if(m_marks, [](const mark &m) { return !m.in_use; });
        if(idle != m_marks.end())
            return *idle;
        return *std::ranges::min_element(m_marks, {}, &mark::sent_at);
    }
};

// The unsubscribe goes out before the drop is reported, so a drop callback that tears the session down sends nothing after it.
template<typename Session>
void note_unsolicited(Session &s, std::uint64_t topic_hash)
{
    if(s.m_unsolicited.due(topic_hash, Session::clock_type::now()))
    {
        const auto req = wire::encode_unsubscribe_request({.topic_hash = topic_hash});
        s.m_payload_scratch.assign(req.begin(), req.end());
        send_control(s, wire::msg_type::unsubscribe);
    }
    report_drop(s, drop_cause::unsolicited, topic_hash);
}

}

#endif
