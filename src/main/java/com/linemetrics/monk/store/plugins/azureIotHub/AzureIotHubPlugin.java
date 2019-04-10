package com.linemetrics.monk.store.plugins.azureIotHub;

import com.google.gson.JsonObject;
import com.linemetrics.monk.config.dao.DataStream;
import com.linemetrics.monk.dao.DataItem;
import com.linemetrics.monk.director.RunnerContext;
import com.linemetrics.monk.processor.ProcessorException;
import com.linemetrics.monk.store.IStore;
import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AzureIotHubPlugin implements IStore {

    private static final String CONNECTION_STRING_KEY = "connection_string";
    private static final String MAPPING_KEY = "mapping";

    @Override
    public boolean initialize(RunnerContext ctx, JSONObject settings, Map<String, String> metaInfos, Map<Integer, Map<String, String>> dataStreamMetaInfos, List<DataStream> ds) {
        return false;
    }

    @Override
    public boolean store(RunnerContext ctx, JSONObject settings, Map<String, String> metaInfos, Map<Integer, Map<String, String>> dataStreamMetaInfos, Map<Integer, List<DataItem>> items) throws ProcessorException {
        String iotHubConnectionString = settings.containsKey(CONNECTION_STRING_KEY) ? (String) settings.get(CONNECTION_STRING_KEY) : null;
        String iotHubMappingString = settings.containsKey(MAPPING_KEY) ? (String) settings.get(MAPPING_KEY) : null;

        if(!checkParameters(iotHubConnectionString, iotHubMappingString)){
            return false;
        }

        Map<String, String> lmToIotHubPropertyMap = convertIotHubMappingStringToMap(iotHubMappingString);
        List<JsonObject> iotHubObjects = convertItemsToIotHubObject(lmToIotHubPropertyMap, items);

        for(JsonObject o : iotHubObjects) {
            System.out.println(o.toString());
        }

        System.out.println("Size: " + iotHubObjects.size());

        System.out.println("Send a single data");
        AzureIotHubSender sender = new AzureIotHubSender(iotHubConnectionString);
        JsonObject obj = iotHubObjects.get(0);
        sender.sendData(iotHubObjects);

        System.out.println("I'm done here!");
        return false;
    }

    private boolean checkParameters(String iotHubConnectionString, String iotHubMappingString) {
        if(StringUtils.isEmpty(iotHubConnectionString)){
            System.out.println("A IoT-Hub connectionString must be provided!");
            return false;
        }
        if(StringUtils.isEmpty(iotHubMappingString)){
            System.out.println("A Linemetrics to IoT-Hub mapping string must be provided");
            return false;
        }
        return true;
    }


    private static Map<String, String> convertIotHubMappingStringToMap(String iotHubMappingString) {
        String[] splittedString = iotHubMappingString.split(";");
        Map<String, String> map = new HashMap<>();

        for (String s : splittedString) {
            String[] splittedS = s.split(",");
            map.put(splittedS[0], splittedS[1]);
        }

        return map;
    }


    private static List<JsonObject> convertItemsToIotHubObject(Map<String, String> lmToIotHubPropertyMap, Map<Integer, List<DataItem>> items) {
        List<JsonObject> iotHubObjects = new ArrayList<>();
        List<DataItem> dataItems = items.get(1);
        for (DataItem item : dataItems) {
            iotHubObjects.add(convertDataItemToIotHubObject(lmToIotHubPropertyMap, item));
        }
        return iotHubObjects;
    }

    private static JsonObject convertDataItemToIotHubObject(Map<String, String> lmToIotHubPropertyMap, DataItem item) {
        JsonObject object = new JsonObject();
        for (String lmKey : lmToIotHubPropertyMap.keySet()) {
            String iotHubKey = lmToIotHubPropertyMap.get(lmKey);
            switch (lmKey) {
                case "value":
                    object.addProperty(iotHubKey, item.getValue());
                    break;
                case "min":
                    object.addProperty(iotHubKey, item.getMin());
                    break;
                case "max":
                    object.addProperty(iotHubKey, item.getMax());
                    break;
                case "timestampStart":
                    object.addProperty(iotHubKey, item.getTimestampStart());
                    break;
            }
        }
        return object;
    }

}
