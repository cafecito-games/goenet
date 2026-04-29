#define ENET_IMPLEMENTATION
#include "harness.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void harness_init_defaults(harness_config *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->host = "127.0.0.1";
    cfg->port = 7777;
    cfg->peer_count = 1;
    cfg->timeout_ms = 5000;
}

void harness_usage(const char *argv0) {
    fprintf(stderr,
            "usage: %s [--host HOST] [--port PORT] [--send DATA] [--expect DATA] "
            "[--peer-count N] [--timeout-ms MS]\n",
            argv0);
}

bool harness_parse_args(int argc, char **argv, harness_config *cfg) {
    harness_init_defaults(cfg);

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            cfg->host = argv[++i];
            continue;
        }
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            long value = strtol(argv[++i], NULL, 10);
            if (value < 0 || value > 65535) {
                fprintf(stderr, "invalid port: %ld\n", value);
                return false;
            }
            cfg->port = (uint16_t)value;
            continue;
        }
        if (strcmp(argv[i], "--send") == 0 && i + 1 < argc) {
            cfg->send_payload = argv[++i];
            continue;
        }
        if (strcmp(argv[i], "--expect") == 0 && i + 1 < argc) {
            cfg->expect_payload = argv[++i];
            continue;
        }
        if (strcmp(argv[i], "--peer-count") == 0 && i + 1 < argc) {
            long value = strtol(argv[++i], NULL, 10);
            if (value < 1 || value > 1024) {
                fprintf(stderr, "invalid peer count: %ld\n", value);
                return false;
            }
            cfg->peer_count = (int)value;
            continue;
        }
        if (strcmp(argv[i], "--timeout-ms") == 0 && i + 1 < argc) {
            long value = strtol(argv[++i], NULL, 10);
            if (value < 1 || value > 60000) {
                fprintf(stderr, "invalid timeout: %ld\n", value);
                return false;
            }
            cfg->timeout_ms = (int)value;
            continue;
        }

        harness_usage(argv[0]);
        return false;
    }

    return true;
}

void harness_log(const char *event, const char *value) {
    if (value == NULL) {
        printf("%s\n", event);
    } else {
        printf("%s %s\n", event, value);
    }
    fflush(stdout);
}

bool harness_payload_matches(const ENetPacket *packet, const char *expect_payload) {
    if (expect_payload == NULL) {
        return true;
    }

    size_t expect_len = strlen(expect_payload);
    return packet->dataLength == expect_len &&
           memcmp(packet->data, expect_payload, packet->dataLength) == 0;
}
