#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int peer_index(ENetPeer **peers, int peer_count, ENetPeer *peer) {
    for (int i = 0; i < peer_count; ++i) {
        if (peers[i] == peer) {
            return i;
        }
    }

    return -1;
}

static void reset_peers(ENetPeer **peers, int peer_count) {
    if (peers == NULL) {
        return;
    }

    for (int i = 0; i < peer_count; ++i) {
        if (peers[i] != NULL && peers[i]->state != ENET_PEER_STATE_DISCONNECTED) {
            enet_peer_reset(peers[i]);
        }
    }
}

static void log_receive(int index, const ENetPacket *packet) {
    char payload[packet->dataLength + 1];
    char message[sizeof(payload) + 32];

    memcpy(payload, packet->data, packet->dataLength);
    payload[packet->dataLength] = '\0';
    snprintf(message, sizeof(message), "%d %s", index, payload);
    harness_log("RECEIVE", message);
}

static int run_client(const harness_config *cfg) {
    if (cfg->expect_payload == NULL) {
        fprintf(stderr, "expected payload is required\n");
        return 1;
    }

    ENetHost *client = enet_host_create(NULL, (size_t)cfg->peer_count, 1, 0, 0);
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

    ENetPeer **peers = calloc((size_t)cfg->peer_count, sizeof(*peers));
    bool *connected = calloc((size_t)cfg->peer_count, sizeof(*connected));
    bool *received = calloc((size_t)cfg->peer_count, sizeof(*received));
    if (peers == NULL || connected == NULL || received == NULL) {
        fprintf(stderr, "allocation failure\n");
        free(received);
        free(connected);
        free(peers);
        enet_host_destroy(client);
        return 1;
    }

    for (int i = 0; i < cfg->peer_count; ++i) {
        peers[i] = enet_host_connect(client, &address, 1, 0);
        if (peers[i] == NULL) {
            fprintf(stderr, "client connect setup failed for peer %d\n", i);
            reset_peers(peers, cfg->peer_count);
            free(received);
            free(connected);
            free(peers);
            enet_host_destroy(client);
            return 1;
        }
    }

    int connected_count = 0;
    int received_count = 0;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts && received_count < cfg->peer_count; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT: {
                    int index = peer_index(peers, cfg->peer_count, event.peer);
                    if (index >= 0 && !connected[index]) {
                        char value[16];
                        snprintf(value, sizeof(value), "%d", index);
                        harness_log("CONNECT", value);
                        connected[index] = true;
                        connected_count++;
                    }
                    break;
                }
                case ENET_EVENT_TYPE_RECEIVE: {
                    int index = peer_index(peers, cfg->peer_count, event.peer);
                    if (index >= 0 &&
                        !received[index] &&
                        harness_payload_matches(event.packet, cfg->expect_payload)) {
                        log_receive(index, event.packet);
                        received[index] = true;
                        received_count++;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                }
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "peer disconnected before broadcast completed\n");
                    attempt = max_attempts;
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }
    }

    reset_peers(peers, cfg->peer_count);
    free(received);
    free(connected);
    free(peers);
    enet_host_destroy(client);
    return connected_count == cfg->peer_count && received_count == cfg->peer_count ? 0 : 1;
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
