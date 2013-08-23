package com.splicemachine.storage;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PredicateUtils {

    public static Map<Integer, List<Predicate>> initPredicateMap( List<Predicate> ands){

        Map<Integer, List<Predicate>> predMap = new HashMap<Integer, List<Predicate>>();

        for(Predicate pred : ands){
            for(Integer col : pred.appliesToColumns()){
                if(predMap.containsKey(col)){
                    predMap.get(col).add(pred);
                }else{
                    List<Predicate> ll = new LinkedList<Predicate>();
                    ll.add(pred);
                    predMap.put(col, ll);
                }
            }
        }

        return predMap;
    }

}
