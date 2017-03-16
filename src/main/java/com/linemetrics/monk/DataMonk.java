package com.linemetrics.monk;

import com.linemetrics.monk.api.ApiManager;
import com.linemetrics.monk.api.IApiClient;
import com.linemetrics.monk.api.auth.HashBasedCredential;
import com.linemetrics.monk.api.auth.ICredentials;
import com.linemetrics.monk.api.auth.SecretBasedCredential;
import com.linemetrics.monk.config.ConfigStoreManager;
import com.linemetrics.monk.config.dao.Api;
import com.linemetrics.monk.config.dao.DirectorJob;
import com.linemetrics.monk.config.file.SystemConfigStore;
import com.linemetrics.monk.director.Director;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.List;

public class DataMonk {

    private static final Logger logger = LoggerFactory.getLogger(DataMonk.class);

    public static final String CONFIG_PATH      = "";
    public static final String CONFIG_SYSTEM    = "system.properties";
//    public static final String CONFIG_SYSTEM    = "system.db";

    public DataMonk() throws Exception {
        ConfigStoreManager.setSystemStore(
            new SystemConfigStore(
                CONFIG_PATH + CONFIG_SYSTEM));
    }

    public void start() throws Exception {

        Api api = ConfigStoreManager.getSystemStore().getApiCredentials();
        System.out.println(api);

        List<DirectorJob> directorJobs = ConfigStoreManager.getSystemStore().getDirectorJobs();
        String apiversion = StringUtils.isNotEmpty(api.getVersion())?api.getVersion():"com.linemetrics.monk.api";

        //credentials based on given properties
        ICredentials credentials = (StringUtils.isNotEmpty(api.getClientId()) && StringUtils.isNotEmpty(api.getClientSecret()))
                                    ? new SecretBasedCredential(api.getClientId(), api.getClientSecret())
                                    : new HashBasedCredential(api.getApiHash());

        Constructor c = Class.forName(apiversion + ".ApiClient").getConstructor(String.class, ICredentials.class);
        IApiClient client = (IApiClient)c.newInstance(api.getEndpoint(), credentials);

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
