#include "test_recording_host_sink_common.h"

using namespace host_sink_fixture;

TEST_CASE("recording_host_sink file_sink is byte_identical to the in-memory sink", "[recording_host_sink][file]")
{
    in_memory_byte_sink mem;
    capture_session(mem);
    const std::vector<std::byte> expected{mem.bytes().begin(), mem.bytes().end()};

    const std::filesystem::path path = unique_path("byte_identical").replace_extension(".plxr");
    {
        file_sink fs{path};
        capture_session(fs);
    }
    const std::vector<std::byte> got = read_file(path);
    std::filesystem::remove(path);

    REQUIRE(got == expected);
    REQUIRE_FALSE(expected.empty());
}

TEST_CASE("recording_host_sink rotating_sink keep_n retention keeps the newest segments", "[recording_host_sink][rotating]")
{
    const std::filesystem::path dir = unique_path("keep_n");
    std::filesystem::create_directories(dir);

    rotating_sink::options opts;
    opts.directory = dir;
    opts.basename  = "cap";
    opts.keep      = 2;

    rotating_sink rs{opts};
    const std::array<std::byte, 4> blob{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    rs.write(blob);
    for(int i = 0; i < 4; ++i)
    {
        rs.rotate();
        rs.write(blob);
    }
    rs.flush();

    REQUIRE(rs.segments().size() == 2);

    std::size_t files = 0;
    for(const auto &entry : std::filesystem::directory_iterator(dir))
        if(entry.is_regular_file())
            ++files;
    REQUIRE(files == 2);

    for(const std::filesystem::path &seg : rs.segments())
        REQUIRE(std::filesystem::exists(seg));

    std::filesystem::remove_all(dir);
}

TEST_CASE("recording_host_sink rotating_sink two-segment capture decodes standalone", "[recording_host_sink][rotating]")
{
    const std::filesystem::path dir = unique_path("twoseg");
    std::filesystem::create_directories(dir);

    rotating_sink::options opts;
    opts.directory = dir;
    opts.basename  = "seg";

    std::vector<std::byte> preamble_bytes;
    {
        rotating_sink rs{opts};
        flat_recorder rec{rs, 64u * 1024u, [n = std::uint64_t{0}]() mutable { return ++n; }};
        rec.open(make_node(5), topic_capture_rule{});
        rs.prime_preamble(rec.preamble());
        preamble_bytes.assign(rec.preamble().begin(), rec.preamble().end());

        recording_sink tap{rec};
        for(int i = 0; i < 3; ++i)
        {
            message_info info{};
            info.publication_sequence = static_cast<std::uint64_t>(i);
            const std::string body = "first-" + std::to_string(i);
            tap.on_message_delivered("seg/a", info, message_view{bytes_of(body), {}});
        }
        while(rec.pump())
            ;
        rec.flush();

        rs.rotate();
        for(int i = 0; i < 3; ++i)
        {
            message_info info{};
            info.publication_sequence = static_cast<std::uint64_t>(100 + i);
            const std::string body = "second-" + std::to_string(i);
            tap.on_message_delivered("seg/a", info, message_view{bytes_of(body), {}});
        }
        while(rec.pump())
            ;
        rec.flush();
    }

    std::vector<std::filesystem::path> segs;
    for(const auto &entry : std::filesystem::directory_iterator(dir))
        if(entry.is_regular_file())
            segs.push_back(entry.path());
    std::sort(segs.begin(), segs.end());
    REQUIRE(segs.size() == 2);

    REQUIRE_FALSE(preamble_bytes.empty());
    for(const std::filesystem::path &seg : segs)
    {
        const std::vector<std::byte> bytes = read_file(seg);
        const auto proj                    = read_projection_input(bytes);
        REQUIRE(proj.has_value());
        REQUIRE_FALSE(proj->records.empty());
    }

    const std::vector<std::byte> second = read_file(segs[1]);
    REQUIRE(second.size() >= preamble_bytes.size());
    REQUIRE(std::equal(preamble_bytes.begin(), preamble_bytes.end(), second.begin()));

    std::filesystem::remove_all(dir);
}
