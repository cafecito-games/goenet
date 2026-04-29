#ifndef GOENET_INTEROP_HARNESS_H
#define GOENET_INTEROP_HARNESS_H

#include <stdbool.h>
#include <stdint.h>

#include "enet.h"

typedef struct {
    const char *host;
    uint16_t port;
    const char *send_payload;
    const char *expect_payload;
    int peer_count;
    int timeout_ms;
} harness_config;

bool harness_parse_args(int argc, char **argv, harness_config *cfg);
void harness_usage(const char *argv0);
void harness_log(const char *event, const char *value);
bool harness_payload_matches(const ENetPacket *packet, const char *expect_payload);

#endif
