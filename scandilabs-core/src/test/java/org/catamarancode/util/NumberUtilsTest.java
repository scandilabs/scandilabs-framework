package org.catamarancode.util;

import java.io.IOException;

import junit.framework.TestCase;

public class NumberUtilsTest extends TestCase {

    public void testToUnsignedInt() throws IOException {
        
        for (int i = -10; i < 10; i++) {
            System.out.println("" + i + ": " + NumberUtils.toUnsignedInt(i));
        }
        
    }
}
