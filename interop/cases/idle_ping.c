#include "harness.h"

#include <stdbool.h>
#include <stdio.h>

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
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT:
                    if (!connected) {
                        harness_log("CONNECT", NULL);
                        connected = true;
                    }
                    break;
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "client disconnected during idle window\n");
                    if (peer->state != ENET_PEER_STATE_DISCONNECTED) {
                        enet_peer_reset(peer);
                    }
                    enet_host_destroy(client);
                    return 1;
                case ENET_EVENT_TYPE_RECEIVE:
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
        }
    }

    if (!connected) {
        fprintf(stderr, "client did not connect before idle window elapsed\n");
        if (peer->state != ENET_PEER_STATE_DISCONNECTED) {
            enet_peer_reset(peer);
        }
        enet_host_destroy(client);
        return 1;
    }

    harness_log("IDLE_OK", NULL);
    enet_peer_reset(peer);
    enet_host_destroy(client);
    return 0;
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
