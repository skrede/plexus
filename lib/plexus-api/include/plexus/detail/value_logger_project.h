#ifndef HPP_GUARD_PLEXUS_DETAIL_VALUE_LOGGER_PROJECT_H
#define HPP_GUARD_PLEXUS_DETAIL_VALUE_LOGGER_PROJECT_H

#include "plexus/value_projection.h"
#include "plexus/value_logger_options.h"

#include "plexus/io/message_info.h"

#include <string>
#include <string_view>

// The per-topic CSV/jsonl/text projection helpers for value_logger, relocated out of the handle.
// Each is a free-function template over the logger's heap state (St holds buffer/slot/projection/
// out/header_written); HasProjection selects the value_projection path vs the operator<< floor.
namespace plexus::detail {

template<bool HasProjection, typename St>
void vl_emit_column_names(St &s)
{
    if constexpr(HasProjection)
        for(std::string_view name : s.projection.columns())
        {
            s.buffer += ',';
            s.buffer += name;
        }
    else
        s.buffer += ",value";
}

template<bool HasProjection, typename St>
void vl_emit_csv_fields(St &s)
{
    if constexpr(HasProjection)
        s.projection.emit_fields(s.slot, s.buffer, ',');
    else
        stream_to(s.slot, s.buffer);
}

template<bool HasProjection, typename St>
void vl_format_csv(St &s, const io::message_info &info)
{
    if(!s.header_written)
    {
        s.buffer.clear();
        s.buffer += "publication_sequence";
        vl_emit_column_names<HasProjection>(s);
        s.buffer += '\n';
        s.out << s.buffer;
        s.header_written = true;
    }
    s.buffer.clear();
    s.buffer += std::to_string(info.publication_sequence);
    s.buffer += ',';
    vl_emit_csv_fields<HasProjection>(s);
    s.buffer += '\n';
}

template<bool HasProjection, typename St>
void vl_format_json(St &s)
{
    s.buffer.clear();
    s.buffer += '{';
    if constexpr(HasProjection)
        s.projection.emit_json(s.slot, s.buffer);
    else
    {
        s.buffer += "\"value\":\"";
        stream_to(s.slot, s.buffer);
        s.buffer += '"';
    }
    s.buffer += "}\n";
}

template<bool HasProjection, typename St>
void vl_format_text(St &s, const io::message_info &info)
{
    s.buffer.clear();
    s.buffer += std::to_string(info.publication_sequence);
    s.buffer += ' ';
    if constexpr(HasProjection)
        s.projection.emit_fields(s.slot, s.buffer, ' ');
    else
        stream_to(s.slot, s.buffer);
    s.buffer += '\n';
}

template<bool HasProjection, typename St>
void vl_format_record(St &s, const io::message_info &info)
{
    switch(s.format)
    {
        case log_format::csv:   vl_format_csv<HasProjection>(s, info); break;
        case log_format::jsonl: vl_format_json<HasProjection>(s); break;
        case log_format::text:  vl_format_text<HasProjection>(s, info); break;
    }
}

}

#endif
