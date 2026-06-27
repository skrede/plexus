#ifndef HPP_GUARD_PLEXUS_EXAMPLE_WIFI_NETIF_H
#define HPP_GUARD_PLEXUS_EXAMPLE_WIFI_NETIF_H

#include "wifi_credentials.h"

#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "nvs_flash.h"
#include "freertos/event_groups.h"

#include <string>
#include <cstring>
#include <cstdint>

namespace example {

inline EventGroupHandle_t g_wifi_events = nullptr;
inline esp_netif_t *g_sta_netif = nullptr;
inline constexpr int k_wifi_got_ip_bit = BIT0;

inline void on_wifi_event(void *, esp_event_base_t base, ::int32_t id, void *)
{
    if(base == WIFI_EVENT && id == static_cast<::int32_t>(WIFI_EVENT_STA_DISCONNECTED))
        esp_wifi_connect();
}

inline void on_ip_event(void *, esp_event_base_t, ::int32_t, void *)
{
    xEventGroupSetBits(g_wifi_events, k_wifi_got_ip_bit);
}

inline void init_nvs()
{
    esp_err_t err = nvs_flash_init();
    if(err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        nvs_flash_erase();
        nvs_flash_init();
    }
}

inline void register_wifi_handlers()
{
    g_wifi_events = xEventGroupCreate();
    esp_event_handler_instance_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &on_wifi_event, nullptr, nullptr);
    esp_event_handler_instance_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &on_ip_event, nullptr, nullptr);
}

inline void start_sta()
{
    wifi_config_t cfg{};
    std::strncpy(reinterpret_cast<char *>(cfg.sta.ssid), WIFI_SSID, sizeof(cfg.sta.ssid));
    std::strncpy(reinterpret_cast<char *>(cfg.sta.password), WIFI_PASSWORD, sizeof(cfg.sta.password));
    esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_set_config(WIFI_IF_STA, &cfg);
    esp_wifi_start();
    esp_wifi_connect();
}

// Bring up the STA netif in example/HAL code, join the AP, and block until a DHCP lease
// arrives — the lwIP stack comes up under esp_netif; plexus only opens sockets afterward.
// esp_netif_create_default_wifi_sta() raises NETIF_FLAG_IGMP on the STA netif (the
// membership-report flag the IGMP join needs), so no explicit netif-flag step is required here.
inline bool wifi_connect_sta()
{
    init_nvs();
    esp_netif_init();
    esp_event_loop_create_default();
    g_sta_netif = esp_netif_create_default_wifi_sta();
    wifi_init_config_t init = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&init);
    register_wifi_handlers();
    start_sta();
    const EventBits_t bits = xEventGroupWaitBits(g_wifi_events, k_wifi_got_ip_bit, pdFALSE, pdTRUE, portMAX_DELAY);
    return (bits & k_wifi_got_ip_bit) != 0;
}

// The DHCP-assigned STA address as a dotted-quad — the multicast egress interface the lwIP leaf
// selects for SEND (IP_MULTICAST_IF), never baked into the firmware.
inline std::string sta_ipv4()
{
    esp_netif_ip_info_t info{};
    if(g_sta_netif == nullptr || esp_netif_get_ip_info(g_sta_netif, &info) != ESP_OK)
        return std::string{"0.0.0.0"};
    char dotted[16]{};
    esp_ip4addr_ntoa(&info.ip, dotted, sizeof(dotted));
    return std::string{dotted};
}

}

#endif
