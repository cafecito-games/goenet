#include "harness.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

// Client that connects to a Go server, then logs the bandwidth caps the
// server advertised in its VerifyConnect response. Used to verify that
// goenet's handleConnect now mirrors enet.h:1972-1973 (advertise host
// incoming/outgoing bandwidth) instead of always sending zeros.
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
    int result = 1;
    int max_attempts = cfg->timeout_ms / 25;
    if (max_attempts < 1) {
        max_attempts = 1;
    }

    for (int attempt = 0; attempt < max_attempts && !connected; ++attempt) {
        ENetEvent event = {0};
        while (enet_host_service(client, &event, 25) > 0) {
            switch (event.type) {
                case ENET_EVENT_TYPE_CONNECT: {
                    char buffer[64];
                    snprintf(
                        buffer,
                        sizeof(buffer),
                        "in=%u out=%u",
                        event.peer->incomingBandwidth,
                        event.peer->outgoingBandwidth);
                    harness_log("BANDWIDTH", buffer);
                    connected = true;
                    result = 0;
                    break;
                }
                case ENET_EVENT_TYPE_DISCONNECT:
                case ENET_EVENT_TYPE_DISCONNECT_TIMEOUT:
                    fprintf(stderr, "client disconnected before connect event\n");
                    attempt = max_attempts;
                    break;
                case ENET_EVENT_TYPE_RECEIVE:
                    enet_packet_destroy(event.packet);
                    break;
                case ENET_EVENT_TYPE_NONE:
                    break;
            }
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
