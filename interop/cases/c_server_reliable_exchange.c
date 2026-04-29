#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

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

    bool connected = false;
    bool payload_sent = false;
    bool payload_received = false;
    ENetPeer *peer = NULL;
    int result = 1;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts && !payload_received; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(server, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    peer = event.peer;
                    connected = true;
                    harness_log("CONNECT", NULL);
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    log_receive(event.packet);
                    if (harness_payload_matches(event.packet, cfg->expect_payload)) {
                        payload_received = true;
                        result = 0;
                    }
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "server disconnected before completion\n");
                    attempt = max_attempts;
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
                fprintf(stderr, "unable to create server packet\n");
                break;
            }
            if (enet_peer_send(peer, 0, packet) != 0) {
                fprintf(stderr, "unable to queue server packet\n");
                enet_packet_destroy(packet);
                break;
            }
            enet_host_flush(server);
            payload_sent = true;
        }
    }

    if (peer != NULL) {
        enet_peer_disconnect_now(peer, 0);
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
