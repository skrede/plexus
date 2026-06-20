#include "test_contact_card_common.h"

using namespace contact_card_fixture;

TEST_CASE("static_discovery notifies a browser registered BEFORE a later advertise",
          "[discovery][static_discovery]")
{
    static_discovery disco{{}};

    std::vector<std::string> seen;
    disco.browse([&](const service_info &svc) { seen.push_back(svc.name); });
    REQUIRE(seen.empty()); // empty table: nothing fired at registration

    service_info late;
    late.name     = "late-joiner";
    late.endpoint = {"tcp", "192.0.2.40:5000"};
    disco.advertise(late);

    REQUIRE(seen.size() == 1);
    REQUIRE(seen.front() == "late-joiner");
}

TEST_CASE("static_discovery notifies EVERY retained browser on a later advertise",
          "[discovery][static_discovery]")
{
    static_discovery disco{{}};

    std::vector<std::string> a, b;
    disco.browse([&](const service_info &svc) { a.push_back(svc.name); });
    disco.browse([&](const service_info &svc) { b.push_back(svc.name); });

    service_info svc;
    svc.name     = "node-z";
    svc.endpoint = {"tcp", "192.0.2.41:5000"};
    disco.advertise(svc);

    REQUIRE(a.size() == 1);
    REQUIRE(b.size() == 1);
    REQUIRE(a.front() == "node-z");
    REQUIRE(b.front() == "node-z");
}

TEST_CASE("static_discovery replaces a same-name entry in place instead of appending a duplicate",
          "[discovery][static_discovery]")
{
    static_discovery disco{{}};

    service_info first;
    first.name     = "node-q";
    first.endpoint = {"tcp", "192.0.2.42:5000"};
    first.metadata = {{"plexus/tcp/port", "5000"}};
    disco.advertise(first);

    service_info updated;
    updated.name     = "node-q"; // SAME name
    updated.endpoint = {"tcp", "192.0.2.42:6000"};
    updated.metadata = {{"plexus/tcp/port", "6000"}};
    disco.advertise(updated);

    // A browser registered after both advertises sees EXACTLY ONE node-q entry,
    // carrying the UPDATED record (replace-by-name, not append).
    std::vector<service_info> resolved;
    disco.browse([&](const service_info &svc) { resolved.push_back(svc); });

    REQUIRE(resolved.size() == 1);
    REQUIRE(resolved.front().name == "node-q");
    REQUIRE(read_transport_port(resolved.front().metadata, "tcp") == 6000);
}

TEST_CASE("static_discovery stop() drops retained browsers so a later advertise notifies none",
          "[discovery][static_discovery]")
{
    static_discovery disco{{}};

    std::vector<std::string> seen;
    disco.browse([&](const service_info &svc) { seen.push_back(svc.name); });
    disco.stop();

    service_info svc;
    svc.name     = "after-stop";
    svc.endpoint = {"tcp", "192.0.2.43:5000"};
    disco.advertise(svc);

    REQUIRE(seen.empty()); // the browser was dropped at stop()
}
