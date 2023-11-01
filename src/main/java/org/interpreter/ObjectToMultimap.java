package org.interpreter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.lang.reflect.Field;

public class ObjectToMultimap {
    public static Multimap<String, Object> convertUsingReflection(Object object) throws IllegalAccessException {
        Multimap<String, Object> map = ArrayListMultimap.create();
        Field[] fields = object.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            map.put(field.getName(), field.get(object));
        }

        return map;
    }
}
