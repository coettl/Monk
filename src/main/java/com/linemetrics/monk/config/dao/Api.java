package com.linemetrics.monk.config.dao;

import com.linemetrics.monk.config.ISystemConfigStore;

public class Api {

    ISystemConfigStore store;

    String endpoint;
    String version;

    String clientId;
    String clientSecret;
    String apiHash;


    public Api(ISystemConfigStore store) {
        this.store = store;
    }

    public Api(ISystemConfigStore store, String endpoint, String apiHash, String clientId, String clientSecret, String version) {
        this(store);

        this.apiHash = apiHash;
        this.endpoint = endpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.version = version;
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

    public String getVersion() {
        return version;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    @Override
    public String toString() {
        return "Api{" +
            "endpoint='" + endpoint + '\'' +
            ", apiHash='" + apiHash + '\'' +
            ", clientId='" + clientId + '\'' +
            ", clientSecret='" + clientSecret + '\'' +
            '}';
    }
}
