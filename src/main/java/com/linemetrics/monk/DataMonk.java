package com.linemetrics.monk;

import com.linemetrics.monk.api.ApiClient;
import com.linemetrics.monk.api.ApiManager;
import com.linemetrics.monk.api.auth.HashBasedCredential;
import com.linemetrics.monk.api.auth.ICredentials;
import com.linemetrics.monk.config.ConfigStoreManager;
import com.linemetrics.monk.config.dao.Api;
import com.linemetrics.monk.config.dao.DirectorJob;
import com.linemetrics.monk.config.file.SystemConfigStore;
import com.linemetrics.monk.director.Director;

import java.util.List;

public class DataMonk {

    public static final String CONFIG_PATH      = "";
    public static final String CONFIG_SYSTEM    = "system.properties";
//    public static final String CONFIG_SYSTEM    = "system.db";

    public DataMonk() throws Exception {

//        ConfigStoreManager.setSystemStore(
//            new SystemConfigStore(
//                CONFIG_PATH + CONFIG_SYSTEM));

        ConfigStoreManager.setSystemStore(
            new SystemConfigStore(
                CONFIG_PATH + CONFIG_SYSTEM));
    }

    public void start() throws Exception {

        Api api = ConfigStoreManager.getSystemStore().getApiCredentials();

        System.out.println(api);

        List<DirectorJob> directorJobs = ConfigStoreManager.getSystemStore().getDirectorJobs();

        ICredentials credentials = new HashBasedCredential(api.getApiHash());
        ApiClient client = new ApiClient(
            api.getEndpoint(),
            credentials);
        ApiManager.setClient(client);

        Director director = new Director();
        Director.setDirectorJobs(directorJobs);
        director.run();
    }

    public static void main(String args[]) throws Exception {
        try {
            (new DataMonk()).start();
            System.out.println("Never should reach this...");
        } catch(Exception exp) {
            exp.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }

}
