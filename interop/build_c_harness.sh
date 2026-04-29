#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
default_enet_root="$script_dir/vendor"
enet_root="${ENET_SOURCE_DIR:-$default_enet_root}"
output="${1:-$script_dir/enet-harness}"

if [[ ! -f "$enet_root/include/enet.h" ]]; then
  echo "expected ENet header at $enet_root/include/enet.h; set ENET_SOURCE_DIR to override" >&2
  exit 1
fi

mkdir -p "$(dirname "$output")"

cc -std=c99 -Wall -Wextra -Wno-unused-parameter -I"$enet_root/include" -x c - -o "$output" <<'EOF'
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#define ENET_IMPLEMENTATION
#include "enet.h"

typedef enum {
    MODE_SERVER,
    MODE_CLIENT
} HarnessMode;

typedef struct {
    HarnessMode mode;
    const char *host;
    uint16_t port;
    const char *send_payload;
    const char *expect_payload;
} HarnessConfig;

static void usage(const char *argv0) {
    fprintf(stderr,
            "usage: %s <server|client> [--host HOST] [--port PORT] [--send DATA] [--expect DATA]\n",
            argv0);
}

static bool parse_args(int argc, char **argv, HarnessConfig *cfg) {
    if (argc < 2) {
        usage(argv[0]);
        return false;
    }

    memset(cfg, 0, sizeof(*cfg));
    cfg->host = "127.0.0.1";
    cfg->port = 7777;

    if (strcmp(argv[1], "server") == 0) {
        cfg->mode = MODE_SERVER;
    } else if (strcmp(argv[1], "client") == 0) {
        cfg->mode = MODE_CLIENT;
    } else {
        usage(argv[0]);
        return false;
    }

    for (int i = 2; i < argc; ++i) {
        if (strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            cfg->host = argv[++i];
            continue;
        }
        if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            long value = strtol(argv[++i], NULL, 10);
            if (value <= 0 || value > 65535) {
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

        usage(argv[0]);
        return false;
    }

    return true;
}

static int run_client(const HarnessConfig *cfg) {
    ENetHost *client = enet_host_create(NULL, 1, 1, 0, 0);
    if (client == NULL) {
        fprintf(stderr, "client host creation failed\n");
        return 1;
    }

    ENetAddress address = {0};
    if (enet_address_set_host(&address, cfg->host) != 0) {
        fprintf(stderr, "unable to resolve host %s\n", cfg->host);
        enet_host_destroy(client);
        return 1;
    }
    address.port = cfg->port;

    ENetPeer *peer = enet_host_connect(client, &address, 1, 0);
    if (peer == NULL) {
        fprintf(stderr, "client connect setup failed\n");
        enet_host_destroy(client);
        return 1;
    }

    bool connected = false;
    bool payload_sent = false;
    bool payload_received = false;
    int result = 1;

    for (int attempt = 0; attempt < 200 && !payload_received; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    connected = true;
                    printf("CONNECT\n");
                    fflush(stdout);
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    printf("RECEIVE %.*s\n", (int) event.packet->dataLength, event.packet->data);
                    fflush(stdout);
                    if (cfg->expect_payload == NULL ||
                        (size_t) event.packet->dataLength == strlen(cfg->expect_payload) &&
                            memcmp(event.packet->data, cfg->expect_payload, event.packet->dataLength) == 0) {
                        payload_received = true;
                        result = 0;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "client disconnected before completion\n");
                    attempt = 200;
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }

        if (connected && !payload_sent && cfg->send_payload != NULL) {
            ENetPacket *packet = enet_packet_create(
                cfg->send_payload,
                strlen(cfg->send_payload),
                ENET_PACKET_FLAG_RELIABLE);
            if (packet == NULL) {
                fprintf(stderr, "unable to create client packet\n");
                break;
            }
            if (enet_peer_send(peer, 0, packet) != 0) {
                fprintf(stderr, "unable to queue client packet\n");
                enet_packet_destroy(packet);
                break;
            }
            enet_host_flush(client);
            payload_sent = true;
        }
    }

    enet_peer_disconnect_now(peer, 0);
    enet_host_destroy(client);
    return result;
}

static int run_server(const HarnessConfig *cfg) {
    ENetAddress address = {0};
    address.host = ENET_HOST_ANY;
    address.port = cfg->port;

    ENetHost *server = enet_host_create(&address, 4, 1, 0, 0);
    if (server == NULL) {
        fprintf(stderr, "server host creation failed\n");
        return 1;
    }

    bool connected = false;
    bool payload_received = false;
    int result = 1;

    for (int attempt = 0; attempt < 200 && !payload_received; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(server, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    connected = true;
                    printf("CONNECT\n");
                    fflush(stdout);
                    if (cfg->send_payload != NULL) {
                        ENetPacket *packet = enet_packet_create(
                            cfg->send_payload,
                            strlen(cfg->send_payload),
                            ENET_PACKET_FLAG_RELIABLE);
                        if (packet == NULL || enet_peer_send(event.peer, 0, packet) != 0) {
                            fprintf(stderr, "unable to queue server packet\n");
                            if (packet != NULL) {
                                enet_packet_destroy(packet);
                            }
                            attempt = 200;
                        } else {
                            enet_host_flush(server);
                        }
                    }
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    printf("RECEIVE %.*s\n", (int) event.packet->dataLength, event.packet->data);
                    fflush(stdout);
                    if (cfg->expect_payload == NULL ||
                        (size_t) event.packet->dataLength == strlen(cfg->expect_payload) &&
                            memcmp(event.packet->data, cfg->expect_payload, event.packet->dataLength) == 0) {
                        payload_received = true;
                        result = connected ? 0 : 1;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }
    }

    enet_host_destroy(server);
    return result;
}

int main(int argc, char **argv) {
    HarnessConfig cfg;
    if (!parse_args(argc, argv, &cfg)) {
        return 2;
    }

    if (enet_initialize() != 0) {
        fprintf(stderr, "enet_initialize failed\n");
        return 1;
    }

    int result = cfg.mode == MODE_SERVER ? run_server(&cfg) : run_client(&cfg);
    enet_deinitialize();
    return result;
}
EOF
