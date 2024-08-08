package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Map;

public class Utils {
    public static Boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /***
     *
     * @param arr
     * @return
     */
    public static String asJsonStr(String[] arr) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.writeValueAsString(arr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> asJsonMap(String jsonStr) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.readValue(jsonStr, new TypeReference<Map<String, String>>(){});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String validateStr(String str) {
        return str.isEmpty() ? Magics.IS_EMPTY.value : str;
    }

    public static int asInt(String val) {
        int result = 0 ;

        try {
            result = Integer.parseInt(val);
        } catch (NumberFormatException e) {
            result = Integer.parseInt(Magics.IS_UNEXPECTED_NUMERIC_VAL.value);
        }

        return result;
    }

    public static float asFloat(String val) {
        float result = 0 ;

        try {
            result = Float.parseFloat(val);
        } catch (NumberFormatException e) {
            result = Float.parseFloat(Magics.IS_UNEXPECTED_NUMERIC_VAL.value);
        }

        return result;
    }
}
