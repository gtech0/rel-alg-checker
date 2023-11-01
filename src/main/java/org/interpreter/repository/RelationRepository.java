package org.interpreter.repository;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.interpreter.entity.Message;
import org.interpreter.entity.User;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.interpreter.ObjectToMultimap.convertUsingReflection;

public class RelationRepository {
    public static Map<String, Set<Multimap<String, Object>>> newMap()
            throws IllegalAccessException {
        Map<String, Set<Multimap<String, Object>>> relationMap = new HashMap<>();

        Multimap<String, Object> userTuple1 = convertUsingReflection(new User(
                "john",
                "123123",
                "group1"
        ));

        Multimap<String, Object> userTuple2 = convertUsingReflection(new User(
                "alex",
                "133123",
                "group2"
        ));

        Multimap<String, Object> userTuple3 = convertUsingReflection(new User(
                "andrew",
                "134123",
                "group1"
        ));

        Multimap<String, Object> userTuple4 = convertUsingReflection(new User(
                "jim",
                "135121",
                "group1"
        ));

        Multimap<String, Object> messageTuple1 = convertUsingReflection(new Message(
                "john",
                "qwerty"
        ));

        Multimap<String, Object> messageTuple2 = convertUsingReflection(new Message(
                "alex",
                "uiop"
        ));

        Set<Multimap<String, Object>> relation1 = ImmutableSet.of(userTuple1, userTuple2);
        Set<Multimap<String, Object>> relation2 = ImmutableSet.of(userTuple1, userTuple3, userTuple4);
        Set<Multimap<String, Object>> relation3 = ImmutableSet.of(messageTuple1, messageTuple2);

        relationMap.put("R1", relation1);
        relationMap.put("R2", relation2);
        relationMap.put("R3", relation3);

        return relationMap;
    }
}
