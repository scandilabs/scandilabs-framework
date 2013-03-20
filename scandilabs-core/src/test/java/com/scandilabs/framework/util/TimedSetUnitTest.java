package com.scandilabs.framework.util;

import com.scandilabs.framework.util.TimedSet;

import junit.framework.TestCase;

public class TimedSetUnitTest extends TestCase {

    public void testInsertsAndTimedRemoval() {
        TimedSet<String> set = new TimedSet<String>(100);

        set.add("HOST.COM");
        assertTrue(set.contains("HOST.COM"));
        set.add("OTHERHOST.COM");
        assertTrue(set.contains("HOST.COM"));
        assertTrue(set.contains("OTHERHOST.COM"));

        try {
            Thread.sleep(1200);
        } catch (InterruptedException e) {
            System.err.println(e);
        }

        assertFalse(set.contains("HOST.COM"));
        assertFalse(set.contains("OTHERHOST.COM"));

        set.add("HOST.COM");

        assertTrue(set.contains("HOST.COM"));

    }

}
