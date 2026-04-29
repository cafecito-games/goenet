#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

enum {
    disconnect_data = 0x55667788u,
};

static void log_disconnect_request(uint32_t data) {
    char value[32];
    snprintf(value, sizeof(value), "%u", data);
    harness_log("DISCONNECT_REQUEST", value);
}

static void log_receive(const ENetPacket *packet) {
    char payload[packet->dataLength + 1];
    memcpy(payload, packet->data, packet->dataLength);
    payload[packet->dataLength] = '\0';
    harness_log("RECEIVE", payload);
}

static int run_server(const harness_config *cfg) {
    ENetAddress address = {
        .host = ENET_HOST_ANY,
        .port = cfg->port,
    };

    ENetHost *server = enet_host_create(&address, 1, 1, 0, 0);
    if (server == NULL) {
        fprintf(stderr, "server host creation failed\n");
        return 1;
    }

    char ready[32];
    snprintf(ready, sizeof(ready), "%u", server->address.port);
    harness_log("READY", ready);

    ENetPeer *peer = NULL;
    bool disconnect_requested = false;
    int result = 1;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(server, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    peer = event.peer;
                    harness_log("CONNECT", NULL);
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    log_receive(event.packet);
                    if (!disconnect_requested && harness_payload_matches(event.packet, cfg->expect_payload)) {
                        enet_peer_disconnect(peer, disconnect_data);
                        enet_host_flush(server);
                        log_disconnect_request(disconnect_data);
                        disconnect_requested = true;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                    harness_log("DISCONNECT", NULL);
                    result = disconnect_requested ? 0 : 1;
                    attempt = max_attempts;
                    break;
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "server disconnect timed out\n");
                    attempt = max_attempts;
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }
    }

    if (peer != NULL && peer->state != ENET_PEER_STATE_DISCONNECTED) {
        enet_peer_reset(peer);
    }
    enet_host_destroy(server);
    return result;
}

int main(int argc, char **argv) {
    harness_config cfg;
    if (!harness_parse_args(argc, argv, &cfg)) {
        return 2;
    }

    if (enet_initialize() != 0) {
        fprintf(stderr, "enet_initialize failed\n");
        return 1;
    }

    int result = run_server(&cfg);
    enet_deinitialize();
    return result;
}
