#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

enum {
    disconnect_data = 0xA1B2C3D4u,
    settle_attempts_after_connect = 4,
};

static void log_cycle_event(const char *event, int cycle) {
    char value[16];
    snprintf(value, sizeof(value), "%d", cycle);
    harness_log(event, value);
}

static void log_receive(const ENetPacket *packet) {
    char payload[packet->dataLength + 1];
    memcpy(payload, packet->data, packet->dataLength);
    payload[packet->dataLength] = '\0';
    harness_log("RECEIVE", payload);
}

typedef enum {
    phase_wait_first_connect,
    phase_wait_first_disconnect,
    phase_wait_second_connect,
    phase_wait_second_payload,
} reconnect_phase;

static int run_client(const harness_config *cfg) {
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

    int cycle = 1;
    reconnect_phase phase = phase_wait_first_connect;
    int settle_attempts = 0;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    log_cycle_event("CONNECT", cycle);
                    if (cycle == 1) {
                        phase = phase_wait_first_disconnect;
                        settle_attempts = settle_attempts_after_connect;
                    } else {
                        phase = phase_wait_second_payload;
                    }
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                    if (phase == phase_wait_first_disconnect && cycle == 1) {
                        log_cycle_event("DISCONNECT", cycle);
                        cycle = 2;
                        phase = phase_wait_second_connect;
                        peer = enet_host_connect(client, &address, 1, 0);
                        if (peer == NULL) {
                            fprintf(stderr, "client reconnect setup failed\n");
                            enet_host_destroy(client);
                            return 1;
                        }
                    }
                    break;
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "disconnect timed out during reconnect cycle\n");
                    if (peer->state != ENET_PEER_STATE_DISCONNECTED) {
                        enet_peer_reset(peer);
                    }
                    enet_host_destroy(client);
                    return 1;
                case ENET_EVENT_TYPE_RECEIVE:
                    if (phase == phase_wait_second_payload &&
                        harness_payload_matches(event.packet, cfg->expect_payload)) {
                        log_receive(event.packet);
                        enet_packet_destroy(event.packet);
                        enet_peer_reset(peer);
                        enet_host_destroy(client);
                        return 0;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }

        if (phase == phase_wait_first_disconnect && settle_attempts > 0) {
            settle_attempts--;
            if (settle_attempts == 0) {
                enet_peer_disconnect(peer, disconnect_data);
                enet_host_flush(client);
            }
        }
    }

    if (peer->state != ENET_PEER_STATE_DISCONNECTED) {
        enet_peer_reset(peer);
    }
    enet_host_destroy(client);
    return 1;
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

    int result = run_client(&cfg);
    enet_deinitialize();
    return result;
}
