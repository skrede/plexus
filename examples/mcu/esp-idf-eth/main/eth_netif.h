#ifndef HPP_GUARD_PLEXUS_EXAMPLE_ETH_NETIF_H
#define HPP_GUARD_PLEXUS_EXAMPLE_ETH_NETIF_H

#include "esp_eth.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_eth_mac_esp.h"
#include "freertos/event_groups.h"

#include <cstdint>

namespace example {

inline EventGroupHandle_t g_eth_events = nullptr;
inline constexpr int k_eth_got_ip_bit = BIT0;

inline void on_eth_got_ip(void *, esp_event_base_t, ::int32_t, void *)
{
    xEventGroupSetBits(g_eth_events, k_eth_got_ip_bit);
}

inline esp_eth_handle_t install_eth_driver()
{
    eth_esp32_emac_config_t emac_cfg = ETH_ESP32_EMAC_DEFAULT_CONFIG();
    eth_mac_config_t mac_cfg         = ETH_MAC_DEFAULT_CONFIG();
    eth_phy_config_t phy_cfg         = ETH_PHY_DEFAULT_CONFIG();
    phy_cfg.phy_addr                 = 1;
    esp_eth_mac_t *mac               = esp_eth_mac_new_esp32(&emac_cfg, &mac_cfg);
    esp_eth_phy_t *phy               = esp_eth_phy_new_generic(&phy_cfg);
    esp_eth_config_t config          = ETH_DEFAULT_CONFIG(mac, phy);
    esp_eth_handle_t handle          = nullptr;
    esp_eth_driver_install(&config, &handle);
    return handle;
}

inline void attach_eth_netif(esp_eth_handle_t handle)
{
    esp_netif_config_t netif_cfg = ESP_NETIF_DEFAULT_ETH();
    esp_netif_t *netif           = esp_netif_new(&netif_cfg);
    esp_eth_netif_glue_handle_t glue = esp_eth_new_netif_glue(handle);
    esp_netif_attach(netif, glue);
}

// Bring up esp_eth for a LAN8720/IP101-class RMII PHY in example/HAL code, then block until a
// DHCP lease arrives. No credentials are needed for Ethernet; plexus only opens sockets afterward.
inline bool eth_connect()
{
    g_eth_events = xEventGroupCreate();
    esp_netif_init();
    esp_event_loop_create_default();
    esp_eth_handle_t handle = install_eth_driver();
    attach_eth_netif(handle);
    esp_event_handler_register(IP_EVENT, IP_EVENT_ETH_GOT_IP, &on_eth_got_ip, nullptr);
    esp_eth_start(handle);
    const EventBits_t bits = xEventGroupWaitBits(g_eth_events, k_eth_got_ip_bit, pdFALSE, pdTRUE, portMAX_DELAY);
    return (bits & k_eth_got_ip_bit) != 0;
}

}

#endif
