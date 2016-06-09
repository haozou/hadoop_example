package com.alpine.hadoop.hcatalog;

import com.google.gson.*;

import java.lang.reflect.Type;

/**
 * Created by Hao on 9/15/15.
 */
public class GsonInterfaceAdapter implements
        JsonSerializer<Object>, JsonDeserializer<Object> {

    private static final String CLASS_META_KEY = "CLASS_META_KEY";

    @Override
    public Object deserialize(JsonElement jsonElement, Type type,
                              JsonDeserializationContext jsonDeserializationContext)
            throws JsonParseException {
        JsonObject jsonObj = jsonElement.getAsJsonObject();
        String className = jsonObj.get(CLASS_META_KEY).getAsString();
        try {
            Class<?> clz = Class.forName(className);
            return jsonDeserializationContext.deserialize(jsonElement, clz);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }

    @Override
    public JsonElement serialize(Object object, Type type,
                                 JsonSerializationContext jsonSerializationContext) {
        JsonElement jsonEle = jsonSerializationContext.serialize(object, object.getClass());
        jsonEle.getAsJsonObject().addProperty(CLASS_META_KEY,
                object.getClass().getCanonicalName());
        return jsonEle;
    }
}
