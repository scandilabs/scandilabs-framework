package org.catamarancode.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An LRU(ish) cache (Least Recently Used) that also expires entries that are
 * too old
 */
public class LRUCache<T, E> {

    class LRUMap<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 201528009L;
        
        private int maxSize;

        LRUMap(int maxSize) {
            super.size();
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            boolean remove = size() > maxSize;
            if (remove) {
                // Also remove the time flag
                insertTimes.remove(eldest.getKey());    
            }            
            return remove;
        }
    }

    private LRUMap<T, E> lruMap;

    private Map<T, Date> insertTimes = new HashMap<T, Date>();

    long timeToLiveMillis;

    public LRUCache(int size, long timeToLiveMillis) {
        lruMap = new LRUMap<T, E>(size);
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public void clear(T key) {
        lruMap.remove(key);
        insertTimes.remove(key);
    }

    /**
     * Returns matching object unless it has expired
     * 
     * @param key
     * @return
     */
    public E get(T key) {
        this.removeAllExpired();
        return lruMap.get(key);
    }
    
    private boolean isExpired(T key) {
        Date insertTime = insertTimes.get(key);
        if (insertTime == null) {
            return false;
        }
        if ((insertTime.getTime() + timeToLiveMillis) < System
                .currentTimeMillis()) {
            return true;
        }
        return false;
    }
    
    /**
     * Removes all expired entries
     */
    private void removeAllExpired() {
        List<T> keyList = new ArrayList<T>();
        keyList.addAll(insertTimes.keySet());
        for (T key : keyList) {
            if (isExpired(key)) {
                clear(key);
            }
        }
        keyList.clear();
    }
    
    /**
     * Add item to cache, after expiring old entries first
     */
    public void put(T key, E value) {
        removeAllExpired();
        insertTimes.put(key, new Date());
        lruMap.put(key, value);        
    }
    
    public int size() {
        if (lruMap.size() != insertTimes.size()) {
            throw new RuntimeException("Map size differs, something is wrong!");
        }
        return lruMap.size();
    }

}

