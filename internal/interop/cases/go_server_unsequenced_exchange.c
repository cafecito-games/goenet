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

  bool payload_received = false;
  int result = 1;
  int max_attempts = cfg->timeout_ms / 25;
  if (max_attempts < 1) {
    max_attempts = 1;
  }

  for (int attempt = 0; attempt < max_attempts && !payload_received;
       ++attempt) {
    ENetEvent event = {0};
    while (enet_host_service(client, &event, 25) > 0) {
      switch (event.type) {
      case ENET_EVENT_TYPE_CONNECT:
        harness_log("CONNECT", NULL);
        break;
      case ENET_EVENT_TYPE_RECEIVE:
        log_receive(event.packet);
        if ((event.packet->flags & ENET_PACKET_FLAG_UNSEQUENCED) == 0) {
          fprintf(stderr, "client expected unsequenced packet flag\n");
          enet_packet_destroy(event.packet);
          enet_peer_disconnect_now(peer, 0);
          enet_host_destroy(client);
          return 1;
        }
        if (harness_payload_matches(event.packet, cfg->expect_payload)) {
          payload_received = true;
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
