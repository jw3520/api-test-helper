package kr.co.jw3520.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Condition {
    private Map<String, String> conditionMap;

    private Condition() {
        conditionMap = new HashMap<String, String>();
    }

    public static Condition newCondition(String key, String value) {
        Condition condition = new Condition();
        condition.addCondition(key, value);
        return condition;
    }

    public Condition addCondition(String key, String value) {
        conditionMap.put(key, value);
        return this;
    }

    public String getQuery() {
        String query = "";

        Iterator<String> iterator = conditionMap.keySet().iterator();
        while(iterator.hasNext()) {
            String key = iterator.next();
            query += key + "='" + conditionMap.get(key) + "'";

            if(iterator.hasNext()) {
                query += " AND ";
            }
        }
        return query;
    }

    public String[] getConditionKeyArr() {
        return conditionMap.keySet().toArray(new String[conditionMap.size()]);
    }

    public String getValue(String key) {
        return conditionMap.get(key);
    }
}
