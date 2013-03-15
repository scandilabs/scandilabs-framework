package org.catamarancode.util;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A non-ordered cache that expires entries older than X millis. Backed by a
 * HashMap. Uses a timer thread to expire old entries.
 */
public class TimedSet<E> implements Set<E> {

    class ExpiryTask extends TimerTask {

        @Override
        public void run() {
            removeAllExpired();
        }        
    }
    
    private static Logger logger = LoggerFactory.getLogger(TimedSet.class
            .getName());
    
    private Timer timer;

    private Map<E, Date> insertTimes = new HashMap<E, Date>();
    
    long timeToLiveMillis;

    public TimedSet(long timeToLiveMillis) {
        this.timeToLiveMillis = timeToLiveMillis;
        timer = new Timer(true);
        timer.schedule(new ExpiryTask(), 1000, 1000);
    }

    /**
     * Add item to cache, after expiring old entries first
     */
    public synchronized boolean add(E entry) {
        return addInternal(entry);
    }

    public synchronized boolean addAll(Collection<? extends E> c) {
        boolean changed = false;
        for (E entry : c) {
            if (addInternal(entry)) {
                changed = true;
            }
        }
        return changed;
    }

    private boolean addInternal(E entry) {
        if (this.contains(entry)) {
            return false;
        }
        insertTimes.put(entry, new Date());
        return true;
    }
    
    public void clear() {
        insertTimes.clear();
    }
    
    private void clear(E entry) {
        insertTimes.remove(entry);
    }

    public boolean contains(Object o) {        
        boolean found = insertTimes.keySet().contains(o);
        return found;
    }

    public boolean containsAll(Collection<?> c) {
        return insertTimes.keySet().containsAll(c);
    }

    public boolean isEmpty() {
        return insertTimes.isEmpty();
    }

    private boolean isExpired(E entry) {
        Date insertTime = insertTimes.get(entry);
        if (insertTime == null) {
            return false;
        }
        if ((insertTime.getTime() + timeToLiveMillis) < System
                .currentTimeMillis()) {
            return true;
        }
        return false;
    }

    public Iterator<E> iterator() {
        return insertTimes.keySet().iterator();
    }

    public Date lastInsertTime(E entry) {
        Date insertTime = insertTimes.get(entry);
        return insertTime;
    }

    public synchronized boolean remove(Object o) {
        boolean found = false;
        if (this.contains(o)) {
            found = true;
        }
        insertTimes.remove(o);
        return found;
    }

    public synchronized boolean removeAll(Collection<?> c) {
        boolean found = false;
        for (Object o : c) {
            if (this.remove(o)) {
                found = true;
            }
        }
        return found;
    }

    /**
     * Removes all expired entries
     */
    private synchronized void removeAllExpired() {
        Set<E> toRemove = new HashSet<E>();
        for (E entry : insertTimes.keySet()) {
            if (isExpired(entry)) {
                toRemove.add(entry);                
            }
        }
        for (E entry : toRemove) {
            logger.debug(String.format("Expired %s", entry.toString()));
            this.clear(entry);    
        }
    }
    
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }    

    public int size() {
        return insertTimes.size();
    }

    public Object[] toArray() {
        return insertTimes.keySet().toArray();
    }

    public <T> T[] toArray(T[] a) {
        return insertTimes.keySet().toArray(a);
    }

}
