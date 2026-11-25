#ifndef HPP_GUARD_TESTS_DTLS_DTLS_TEST_SUPPORT_H
#define HPP_GUARD_TESTS_DTLS_DTLS_TEST_SUPPORT_H

// Shared fixtures for the DTLS live tests, grouped by responsibility: the EC P-256
// self-signed identity + credential machinery, the programmable loss-injecting RAW-DTLS
// relay, and the io_context pumps.

#include "dtls_pumps.h"
#include "dtls_relay.h"
#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_channel.h"

#include "plexus/io/endpoint.h"

#endif
