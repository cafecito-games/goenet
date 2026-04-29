#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static const char *client_payloads[] = {
    "c-order-1",
    "c-order-2",
    "c-order-3",
};

static const char *server_payloads[] = {
    "go-order-1",
    "go-order-2",
    "go-order-3",
};

static void log_receive(const ENetPacket *packet) {
    char payload[packet->dataLength + 1];
    memcpy(payload, packet->data, packet->dataLength);
    payload[packet->dataLength] = '\0';
    harness_log("RECEIVE", payload);
}

static bool send_reliable_payloads(ENetHost *client, ENetPeer *peer, const char **payloads, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        ENetPacket *packet = enet_packet_create(payloads[i], strlen(payloads[i]), ENET_PACKET_FLAG_RELIABLE);
        if (packet == NULL) {
            fprintf(stderr, "unable to create reliable packet %zu\n", i);
            return false;
        }
        if (enet_peer_send(peer, 0, packet) != 0) {
            fprintf(stderr, "unable to queue reliable packet %zu\n", i);
            enet_packet_destroy(packet);
            return false;
        }
    }

    enet_host_flush(client);
    return true;
}

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

    bool connected = false;
    bool payloads_sent = false;
    size_t received_count = 0;
    int result = 1;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts && received_count < 3; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    connected = true;
                    harness_log("CONNECT", NULL);
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    log_receive(event.packet);
                    if (received_count >= 3) {
                        fprintf(stderr, "received unexpected extra payload\n");
                        enet_packet_destroy(event.packet);
                        attempt = max_attempts;
                        break;
                    }
                    if ((event.packet->flags & ENET_PACKET_FLAG_RELIABLE) == 0) {
                        fprintf(stderr, "expected reliable packet %zu\n", received_count);
                        enet_packet_destroy(event.packet);
                        attempt = max_attempts;
                        break;
                    }
                    if (!harness_payload_matches(event.packet, server_payloads[received_count])) {
                        fprintf(stderr, "out-of-order payload at index %zu\n", received_count);
                        enet_packet_destroy(event.packet);
                        attempt = max_attempts;
                        break;
                    }
                    received_count++;
                    if (received_count == 3) {
                        result = 0;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "client disconnected before completion\n");
                    attempt = max_attempts;
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }

        if (connected && !payloads_sent) {
            if (!send_reliable_payloads(client, peer, client_payloads, 3)) {
                break;
            }
            payloads_sent = true;
        }
    }

    enet_peer_disconnect_now(peer, 0);
    enet_host_destroy(client);
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

    int result = run_client(&cfg);
    enet_deinitialize();
    return result;
}
