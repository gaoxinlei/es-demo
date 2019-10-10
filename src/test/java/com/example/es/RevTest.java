package com.example.es;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RevTest {

    @Test
    public void testFindDeps(){
        //dep map
        Map<String, Set<String>> depMap = initDepMap();
        //revMap
        Map<String,Set<String>> revMap = reverseMap(depMap);
        List<Node> result = findDeps(revMap,new HashSet<String>(Arrays.asList("4","5","3")));
        printDeps(result,0);
    }

    private static void printDeps(List<Node> result,int depth) {
        for (Node node : result) {
            for(int i=0;i<depth;i++){
                System.out.print("-");
            }
            String key = node.key;
            System.out.println(key);
            List<Node> deps = node.deps;
            if(!deps.isEmpty()){
                printDeps(deps,depth+1);
            }
        }

    }

    private static List<Node> findDeps(Map<String, Set<String>> revMap, Set<String> target) {
        List<Node> result = new ArrayList<>();
        Map<String,Node> map = new HashMap<>();
        Set<String> finished = new HashSet<>();
        findRoot(revMap, target, result,  map, finished);
        return result;
    }

    private static void findRoot(Map<String, Set<String>> revMap, Set<String> target, List<Node> result, Map<String, Node> nodeMap, Set<String> finished) {
        Set<String> nextTarget = new HashSet<>();
        target.forEach(t->{

            if(!finished.contains(t)){
                Node node = nodeMap.computeIfAbsent(t,(k)->{
                    Node n = new Node();
                    n.key = t;
                    return n;
                });
                Set<String> parents = revMap.get(t);
                if(!parents.isEmpty()){
                    nextTarget.addAll(parents);
                    parents.forEach(parent->{
                        if(!nodeMap.containsKey(parent)){
                            Node p = new Node();
                            p.key = parent;
                            nodeMap.put(parent,p);
                        }
                        nodeMap.get(parent).deps.add(node);
                    });
                }else{
                    result.add(node);
                }
            }

        });
        finished.addAll(target);
        if(!nextTarget.isEmpty()){
            findRoot(revMap,nextTarget,result,nodeMap,finished);
        }
    }

    private static class Node{
        String key;
        List<Node> deps = new ArrayList<>();

    }

    private static Map<String, Set<String>> reverseMap(Map<String, Set<String>> depMap) {
        Map<String,Set<String>> reverseMap = new HashMap<>();
        for(Map.Entry<String,Set<String>> entry:depMap.entrySet()){
            String key = entry.getKey();
            reverseMap.computeIfAbsent(key,k->new HashSet<>());
            Set<String> value = entry.getValue();
            value.forEach(dep->{
                reverseMap.computeIfAbsent(dep,(k)->new HashSet<>());
                reverseMap.get(dep).add(key);
            });

        }
        return reverseMap;
    }

    private static Map<String,Set<String>> initDepMap() {
        Map<String, Set<String>> depMap = new HashMap<>();
        depMap.put("1",new HashSet<>());
        depMap.put("2",new HashSet<>());
        depMap.put("3",new HashSet<>());
        depMap.put("4",new HashSet<>());
        depMap.put("5",new HashSet<>());
        depMap.put("6",new HashSet<>());
        depMap.put("7",new HashSet<>());
        depMap.put("8",new HashSet<>());
        depMap.put("9",new HashSet<>());
        depMap.put("10",new HashSet<>());
        depMap.get("1").addAll(Arrays.asList("2","3","4"));
        depMap.get("5").addAll(Arrays.asList("2","6","7"));
        depMap.get("2").add("6");
        depMap.get("4").add("7");
        depMap.get("7").add("3");
        depMap.get("8").addAll(Arrays.asList("9","10","5"));
        depMap.get("10").addAll(Arrays.asList("5","4","6"));
        return depMap;
    }
}
