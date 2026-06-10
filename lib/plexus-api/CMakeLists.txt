# Consumable facade target (the name::name library). Re-exports the minimal
# plexus::core engine; later plans add its include/ directory together with the
# first facade header so every referenced path stays real.
add_library(plexus-api INTERFACE)
add_library(plexus::plexus ALIAS plexus-api)
set_target_properties(plexus-api PROPERTIES EXPORT_NAME plexus)

target_link_libraries(plexus-api INTERFACE plexus::core)

target_compile_features(plexus-api INTERFACE cxx_std_20)
