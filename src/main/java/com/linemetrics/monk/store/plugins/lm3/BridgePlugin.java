package com.linemetrics.monk.store.plugins.lm3;

import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.helper.JsonParser;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.json.simple.JSONObject;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BridgePlugin implements IStore {

    @Override
    public boolean store(RunnerContext ctx,
                         JSONObject settings,
                         final Map<String, String> metaInfos,
                         final Map<Integer, Map<String, String>> dataStreamMetaInfos,
                         Map<Integer, List<DataItem>> items)
        throws ProcessorException {

        String connectionUrl =
            settings.containsKey("connection_url")
                ? (String)settings.get("connection_url") : null;

        String oauthClientId =
            settings.containsKey("oauth_client_id")
                ? (String)settings.get("oauth_client_id") : null;

        String oauthClientSecret =
            settings.containsKey("oauth_client_secret")
                ? (String)settings.get("oauth_client_secret") : null;

        String oauthClientUrl =
            settings.containsKey("oauth_client_url")
                ? (String)settings.get("oauth_client_url") : null;

        Integer itemsPerRequest =
            settings.containsKey("items_per_request")
                ? Integer.valueOf((String)settings.get("items_per_request")) : 256;

        RestFacade restFacade = null;

        try {
            restFacade = new RestFacade(oauthClientId, oauthClientSecret, oauthClientUrl);
        } catch(Exception exp) {
            throw new ProcessorException("Unable to initialize rest client: " + exp.getMessage());
        }

        long time = System.currentTimeMillis();
        int itemCnt = 0;

        for(final Map.Entry<Integer, List<DataItem>> dataStream : items.entrySet()) {

            if( ! dataStreamMetaInfos.containsKey(dataStream.getKey()) ||
                ! dataStreamMetaInfos.get(dataStream.getKey()).containsKey("custom_key") ||
                ! dataStreamMetaInfos.get(dataStream.getKey()).containsKey("alias")) {

                System.out.println("Ignore Rest API Forwarding of DataStream " + dataStream.getKey() + " because of missing custom-key and/or alias!" );
                continue;
            }

            String customKey    = dataStreamMetaInfos.get(dataStream.getKey()).get("custom_key");
            String alias        = dataStreamMetaInfos.get(dataStream.getKey()).get("alias");

            URI restUri;
            try {
                URL restUrl = new URL(connectionUrl + customKey + "/" + alias);
                restUri = restUrl.toURI();
            } catch(Exception exp) {
                System.out.println("Ignore Rest API Forwarding of DataStream " + dataStream.getKey() + " because of wrong formatted url!" );
                continue;
            }

            List<Map> restElements = new ArrayList<>();
            int batch = 0;

            for (final DataItem item : dataStream.getValue()) {
                if(restElements.size() == itemsPerRequest) {
                    batch++;
                    try {
                        restFacade.send(restUri, JsonParser.getInstance().toJson(restElements));
                    } catch(Exception exp) {
                        System.out.println("Error Sending batch #" + batch + " of DataStream " + dataStream.getKey() + ":" + exp.getMessage() );
                        exp.printStackTrace();
                    }
                    restElements.clear();
                }

                restElements.add(new HashMap(){{
                    put("ts", item.getTimestamp());
                    put("val", item.getValue());
                    put("min", item.getValue());
                    put("max", item.getValue());
                }});

                itemCnt++;
            }

            if(restElements.size() > 0) {
                batch++;
                try {
                    restFacade.send(restUri, JsonParser.getInstance().toJson(restElements));
                } catch(Exception exp) {
                    System.out.println("Error Sending batch #" + batch + " of DataStream " + dataStream.getKey() + ":" + exp.getMessage() );
                    exp.printStackTrace();
                }
                restElements.clear();
            }
        }

        System.out.format("Completed sending of %d items took %.2f seconds!", itemCnt, ((0.0 + System.currentTimeMillis()) - time) / 1000);

        return true;
    }
}
