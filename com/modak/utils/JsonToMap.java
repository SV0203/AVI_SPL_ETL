package com.modak.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class JsonToMap {
    static String jsonString = null;

    public static Map<String, Object> jsonString2Map(String jsonString) throws JSONException {
        Map<String, Object> keys = new HashMap();
        JSONObject jsonObject = new JSONObject(jsonString);
        Iterator<?> keyset = jsonObject.keys();
        while (keyset.hasNext()) {
            String key = (String)keyset.next();
            Object value = jsonObject.get(key);
            if ((value instanceof JSONObject)) {
                keys.put(key, jsonString2Map(value.toString()));
            }
            else if ((value instanceof JSONArray)) {
                JSONArray jsonArray = jsonObject.getJSONArray(key);
                keys.put(key, jsonArray2List(jsonArray));
            }
            else {
                keys.put(key, value);
            }
        }
        return keys;
    }

    public static List<Object> jsonArray2List(JSONArray arrayOFKeys) throws JSONException {
        List<Object> array2List = new ArrayList();
        for (int i = 0; i < arrayOFKeys.length(); i++) {
            if ((arrayOFKeys.opt(i) instanceof JSONObject)) {
                Map<String, Object> subObj2Map = jsonString2Map(arrayOFKeys.opt(i).toString());
                array2List.add(subObj2Map);
            } else if ((arrayOFKeys.opt(i) instanceof JSONArray)) {
                List<Object> subarray2List = jsonArray2List((JSONArray)arrayOFKeys.opt(i));
                array2List.add(subarray2List);
            } else {
                array2List.add(arrayOFKeys.opt(i));
            }
        }
        return array2List;
    }

    public static void displayJSONMAP(Map<String, Object> allKeys) throws Exception {
        Set<String> keyset = allKeys.keySet();
        if (!keyset.isEmpty()) {
            Iterator<String> keys = keyset.iterator();
            while (keys.hasNext()) {
                String key = (String)keys.next();
                Object value = allKeys.get(key);
                if ((value instanceof Map)) {
                    displayJSONMAP(jsonString2Map(value.toString()));
                } else if ((value instanceof List)) {
                    JSONArray jsonArray = new JSONArray(value.toString());
                    jsonArray2List(jsonArray);
                }
            }
        }
    }
}
