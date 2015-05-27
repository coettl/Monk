package com.linemetrics.monk.store;

import java.util.HashMap;
import java.util.Map;

public class StoreFactory {

    private static Map<String, IStore> stores = new HashMap<>();

    public static IStore getStore(String type)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(stores.containsKey(type)) {
            return stores.get(type);
        }
        IStore store = (IStore) Class.forName(type).newInstance();
        stores.put(type, store);
        return store;
    }

}
