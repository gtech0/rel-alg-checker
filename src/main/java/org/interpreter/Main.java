package org.interpreter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import net.objecthunter.exp4j.Expression;
import net.objecthunter.exp4j.ExpressionBuilder;
import org.interpreter.exception.BaseException;

import java.util.*;
import java.util.stream.Collectors;

import static org.interpreter.repository.RelationRepository.newMap;

public class Main {
    static Map<String, Set<Multimap<String, Object>>> relationMap;

    public static void main(String[] args) throws IllegalAccessException {
        relationMap = newMap();

//        System.out.println("Relation1: " + relationMap.get("R1"));
//        System.out.println("Relation2: " + relationMap.get("R2"));
//        System.out.println("Relation3: " + relationMap.get("R3"));
//
//        System.out.println("Union: " +
//                Sets.union(relationMap.get("R1"), relationMap.get("R2")));
//
//        System.out.println("Difference: " +
//                Sets.difference(relationMap.get("R1"), relationMap.get("R2")));
//
//        System.out.println("Cartesian product: " +
//                Sets.cartesianProduct(relationMap.get("R1"), relationMap.get("R2")));
//
//        System.out.println("Cartesian product (merged): " +
//                product(relationMap.get("R1"), relationMap.get("R2")));
//
//        System.out.println("Projection: " +
//                projection(relationMap.get("R1"), List.of("username")));
//
//        System.out.println("Intersection: " +
//                Sets.intersection(relationMap.get("R1"), relationMap.get("R2")));
//
//        System.out.println("Division: " +
//                division(relationMap.get("R1"), relationMap.get("R3"), List.of("username")));
//
//        System.out.println("Join: " +
//                join(relationMap.get("R1"), relationMap.get("R2"), List.of("group", "phone", "username")));
//
//        System.out.println("Selection: " +
//                selection(relationMap.get("R1"), List.of("phone", "!=", "133123", "AND", "1", "=", "1")));

        List<String> query1 = Arrays.asList(
                "SELECT R1 WHERE phone != \"133123\" AND 1 = 1 -> T1",
                "DIFFERENCE R1 AND T1 -> T2",
                "DIVIDE R3 BY T2 OVER group username -> T3",
                "JOIN T2 AND T3 OVER username"
        );

        System.out.println("Query: ");
        for (String str : query1) {
            System.out.println(str);
        }
        System.out.println();

        System.out.println("Query output: " + inputProcessing(query1));

        System.out.println("Relations: ");
        for (String key : relationMap.keySet()) {
            System.out.println(key + ": " + relationMap.get(key));
        }
    }

    public static Set<Multimap<String, Object>> inputProcessing(List<String> query) {
        if (query.isEmpty()) {
            return new HashSet<>();
        }

        Iterator<String> queryIterator = query.iterator();
        while (queryIterator.hasNext()) {
            String operation = queryIterator.next();
            String[] tokenizedOperation = operation.split("\\s+");

            int lastIndex = tokenizedOperation.length - 1;

            int lastAttribute;
            if (queryIterator.hasNext())
                lastAttribute = lastIndex - 1;
            else
                lastAttribute = lastIndex + 1;

            Set<Multimap<String, Object>> result;
            //try {
                switch (tokenizedOperation[0]) {
                    case "UNION":
                        result = Sets.union(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3])
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "DIFFERENCE":
                        result = Sets.difference(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3])
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "TIMES":
                        result = product(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3])
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "INTERSECT":
                        result = Sets.intersection(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3])
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "PROJECT":
                        result = projection(
                                relationMap.get(tokenizedOperation[1]),
                                Arrays.stream(Arrays.copyOfRange(tokenizedOperation, 3, lastAttribute)).toList()
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "SELECT":
                        result = selection(
                                relationMap.get(tokenizedOperation[1]),
                                Arrays.stream(Arrays.copyOfRange(tokenizedOperation, 3, lastAttribute)).toList()
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "DIVIDE":
                        result = division(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3]),
                                Arrays.stream(Arrays.copyOfRange(tokenizedOperation, 5, lastAttribute)).toList()
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    case "JOIN":
                        result = join(
                                relationMap.get(tokenizedOperation[1]),
                                relationMap.get(tokenizedOperation[3]),
                                Arrays.stream(Arrays.copyOfRange(tokenizedOperation, 5, lastAttribute)).toList()
                        );

                        if (queryIterator.hasNext())
                            relationMap.put(tokenizedOperation[lastIndex], result);
                        else
                            return result;
                        break;
                    default:
                        break;
                }
//            } catch (Exception e) {
//                throw new BaseException("Exception occurred while executing " + tokenizedOperation[0] + " operation");
//            }
        }
        return new HashSet<>();
    }

    public static Set<Multimap<String, Object>> product(Set<Multimap<String, Object>> relation1,
                                                        Set<Multimap<String, Object>> relation2) {
        Set<List<Multimap<String, Object>>> cartMap = Sets.cartesianProduct(relation1, relation2);
        return cartMap
                .stream()
                .map(maps -> {
                    Multimap<String, Object> newMap = ArrayListMultimap.create();
                    maps.forEach(newMap::putAll);
                    return newMap;
                })
                .collect(Collectors.toSet());
    }

    public static Set<Multimap<String, Object>> projection(Set<Multimap<String, Object>> relation,
                                                           List<String> attributes) {
        return relation
                .stream()
                .map(map -> {
                    Multimap<String, Object> newMap = ArrayListMultimap.create();
                    attributes.forEach(
                            attribute -> map.get(attribute).forEach(
                                    value -> newMap.put(attribute, value)
                            )
                    );
                    return newMap;
                })
                .collect(Collectors.toSet());
    }

    public static Set<Multimap<String, Object>> division(Set<Multimap<String, Object>> relation1,
                                                         Set<Multimap<String, Object>> relation2,
                                                         List<String> uniqueAttributes) {
        Set<Multimap<String, Object>> temp1 = product(projection(relation1, uniqueAttributes), relation2);
        Set<Multimap<String, Object>> temp2 = Sets.difference(temp1, relation1);
        Set<Multimap<String, Object>> temp3 = projection(temp2, uniqueAttributes);
        Set<Multimap<String, Object>> temp4 = Sets.difference(projection(relation1, uniqueAttributes), temp3);
        return temp4;
    }

    public static Set<Multimap<String, Object>> join(Set<Multimap<String, Object>> relation1,
                                                     Set<Multimap<String, Object>> relation2,
                                                     List<String> uniqueAttributes) {
        Set<Multimap<String, Object>> joinedRelation = new HashSet<>();
        Set<Multimap<String, Object>> product = product(relation1, relation2);
        if (uniqueAttributes.isEmpty()) {
            return product;
        } else {
            for (Multimap<String, Object> multimap : product) {
                int joinedAttributes = 0;
                for (String attribute : uniqueAttributes) {
                    Collection<Object> content = multimap.get(attribute);
                    if (content.stream().distinct().count() == 1 && content.size() > 1) {
                        joinedAttributes++;
                        for (Object first : new HashSet<>(content)) {
                            multimap.remove(attribute, first);
                        }
                    }
                }

                if (joinedAttributes == uniqueAttributes.size()) {
                    joinedRelation.add(multimap);
                }
            }
        }
        return joinedRelation;
    }

    public static Set<Multimap<String, Object>> selection(Set<Multimap<String, Object>> relation, List<String> tokens) {
        Queue<String> RPN = shuntingYard(relation, tokens);

        Stack<Object> results = new Stack<>();
        for (String token : RPN) {
            boolean notCheck = false;
            Object op1;
            Object op2;
            String finalOp1;
            String finalOp2;
            Set<Multimap<String, Object>> result;
            //try {
                switch (token) {
                    case "=":
                    case "!=":
                    case ">":
                    case "<":
                    case ">=":
                    case "<=":
                        op1 = results.pop();
                        if (Objects.equals(results.peek(), "NOT")) {
                            notCheck = true;
                            results.pop();
                        }
                        boolean finalNotCheck1 = notCheck;

                        op2 = results.pop();
                        if (Objects.equals(op2, op1)) {
                            results.push(relation);
                        } else {
                            result = new HashSet<>();
                            finalOp2 = (String) op2;
                            finalOp1 = (String) op1;
                            relation.forEach(map ->
                                    map.get(finalOp2).forEach(
                                            value -> valueComparator(token, map, (String) value, finalOp1, finalNotCheck1, result)
                                    )
                            );
                            results.push(result);
                        }
                        break;
                    case "+":
                    case "-":
                    case "*":
                    case "/":
                        op1 = results.pop();
                        op2 = results.pop();
                        finalOp1 = (String) op1;
                        finalOp2 = (String) op2;
                        if (isNumeric(finalOp1) && isNumeric(finalOp2)) {
                            Expression expression = new ExpressionBuilder(finalOp2 + token + finalOp1).build();
                            results.push(Double.toString(expression.evaluate()));
                        }
                        break;
                    case "OR":
                        op1 = results.pop();
                        op2 = results.pop();
                        result = Sets.union((Set<Multimap<String, Object>>) op2, (Set<Multimap<String, Object>>) op1);
                        results.push(result);
                        break;
                    case "AND":
                        op1 = results.pop();
                        op2 = results.pop();
                        result = Sets.intersection((Set<Multimap<String, Object>>) op2, (Set<Multimap<String, Object>>) op1);
                        results.push(result);
                        break;
                    default:
                        results.push(token);
                }
//            } catch (Exception e) {
//                throw new BaseException("Incorrect SELECT syntax");
//            }
        }

        if (results.empty()) {
            return new HashSet<>();
        }
        return (Set<Multimap<String, Object>>) results.firstElement();
    }

    private static void valueComparator(
            String token,
            Multimap<String, Object> map,
            String value,
            String op1,
            boolean finalNotCheck1,
            Set<Multimap<String, Object>> result
    ) {
        double currentVal = Double.parseDouble(value);
        double newVal = Double.parseDouble(op1);
        if ((Objects.equals(token, ">")
                && (currentVal > newVal && !finalNotCheck1 || currentVal <= newVal && finalNotCheck1))

                || (Objects.equals(token, "<")
                && (currentVal < newVal && !finalNotCheck1 || currentVal >= newVal && finalNotCheck1))

                || (Objects.equals(token, ">=")
                && (currentVal >= newVal && !finalNotCheck1 || currentVal < newVal && finalNotCheck1))

                || (Objects.equals(token, "<=")
                && (currentVal <= newVal && !finalNotCheck1 || currentVal > newVal && finalNotCheck1))

                || (Objects.equals(token, "=")
                && (currentVal == newVal && !finalNotCheck1 || currentVal != newVal && finalNotCheck1))

                || (Objects.equals(token, "!=")
                && (currentVal != newVal && !finalNotCheck1 || currentVal == newVal && finalNotCheck1))
        ) {
            result.add(map);
        }
    }

    public static Queue<String> shuntingYard(Set<Multimap<String, Object>> relation, List<String> tokens) {
        Queue<String> outputQueue = new LinkedList<>();
        Stack<String> operatorStack = new Stack<>();
        Map<String, Integer> operators = Map.ofEntries(
                Map.entry("AND", 1),
                Map.entry("OR", 1),
                Map.entry("=", 2),
                Map.entry("!=", 2),
                Map.entry(">", 2),
                Map.entry("<", 2),
                Map.entry(">=", 2),
                Map.entry("<=", 2),
                Map.entry("NOT", 3),
                Map.entry("+", 4),
                Map.entry("-", 4),
                Map.entry("*", 5),
                Map.entry("/", 5)
        );

        for (String token : tokens) {
//            if (isNumeric(token)) {
//                outputQueue.offer(token);
//            } else {
//                for (Multimap<String, Object> multimap : relation) {
//                    if (multimap.containsKey(token) || multimap.containsValue(token)) {
//                        outputQueue.offer(token);
//                        break;
//                    }
//                }
//            }

            if (operators.containsKey(token)) {
                while (
                        !operatorStack.empty()
                                && operators.get(token) <=
                                operators.get(operatorStack.peek())
                ) {
                    outputQueue.offer(operatorStack.pop());
                }
                operatorStack.push(token);
            } else {
                outputQueue.offer(token);
            }
        }

        while (!operatorStack.empty()) {
            outputQueue.offer(operatorStack.pop());
        }

        return outputQueue;
    }

    public static boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        try {
            Double.parseDouble(str);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }
}