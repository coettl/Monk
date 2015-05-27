package com.linemetrics.monk.config;

public class ConfigStoreManager {

    static ISystemConfigStore systemConfigStore;

    public static void setSystemStore(ISystemConfigStore configStore) {
        ConfigStoreManager.systemConfigStore = configStore;
    }

    public static ISystemConfigStore getSystemStore() {
        return ConfigStoreManager.systemConfigStore;
    }
}
