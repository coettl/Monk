package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;

public class Api {

    ISystemConfigStore store;

    String endpoint;
    String apiHash;

    public Api(ISystemConfigStore store) {
        this.store = store;
    }

    public Api(ISystemConfigStore store, String endpoint, String apiHash) {
        this(store);

        this.endpoint = endpoint;
        this.apiHash = apiHash;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getApiHash() {
        return apiHash;
    }

    public void setApiHash(String apiHash) {
        this.apiHash = apiHash;
    }

    @Override
    public String toString() {
        return "Api{" +
            "endpoint='" + endpoint + '\'' +
            ", apiHash='" + apiHash + '\'' +
            '}';
    }
}
