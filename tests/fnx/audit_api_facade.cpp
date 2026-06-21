// The api-facade -fno-exceptions audit translation unit (FNX-02 / FNX-03).
//
// This TU compiles the node facade under -fno-exceptions -fno-rtti to exercise the three
// api-side seams the seam pass converted:
//   - the duplicate-serve fail_closed contract (node serve_procedure_seam, wired into the
//     endpoint seam table at construction),
//   - the node dtor's catch(...) cleanup, now guarded under #if __cpp_exceptions,
//   - the publisher retire cleanup (publisher_publish.h), pulled transitively via publisher.h
//     and likewise guarded.
// Including publisher.h pulls node.h + publisher_publish.h, so a single node construct/destruct
// over the inproc backend instantiates the seam table and the guarded cleanup bodies. The proof
// is structural (compile + link under -fno-exceptions); no flow is driven.

#include "plexus/publisher.h"
#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/node_id.h"

#include <cstddef>

int main()
{
    using policy    = plexus::inproc::inproc_policy;
    using transport = plexus::inproc::inproc_transport<>;
    using node_t    = plexus::node<policy, transport>;

    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   executor{bus};
    transport                           tr{executor, bus};
    plexus::discovery::static_discovery disc{{}};

    plexus::node_id self{};
    self[0] = std::byte{0x01};

    plexus::node_options opts;

    // Construct + destruct: the ctor builds the endpoint seam table (instantiating the
    // converted serve_procedure_seam fail_closed body), and the dtor runs the
    // #if __cpp_exceptions-guarded teardown.
    { node_t node{executor, disc, self, tr, opts}; }

    return 0;
}
