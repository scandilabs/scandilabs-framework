package com.scandilabs.framework.util;

import java.io.IOException;

import com.scandilabs.framework.util.NumberUtils;

import junit.framework.TestCase;

public class NumberUtilsTest extends TestCase {

    public void testToUnsignedInt() throws IOException {
        
        for (int i = -10; i < 10; i++) {
            System.out.println("" + i + ": " + NumberUtils.toUnsignedInt(i));
        }
        
    }
}
