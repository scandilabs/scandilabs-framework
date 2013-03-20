package com.scandilabs.framework.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

public class NumberUtils {

    private NumberUtils() {
    }

    public static long toUnsignedInt(int n) {
        String s = Integer.toHexString(n);
        return Long.parseLong(s, 16);
    }

    /**
     * Converts a string to a long value that is fairly evenly distributed
     * between Long.MIN_VALUE and Long.MAX_VALUE. Internally it uses the 64
     * least significant bits of a MD5 hash. The generated long value is not
     * suitable as a hash because it is not guaranteed to be unique.
     * 
     * @param s
     * @return
     */
    public static long stringToLongCode(String s) {
        String md5 = DigestUtils.md5Hex(s);
        String md5LeastSignificant16 = md5.substring(16, md5.length());
        
        long l = hexToLong(md5LeastSignificant16);
        return l;
    }
    
    /**
     * Converts a string to an evenly distributed unsigned 30-bit integer in the range 0 to 2^30
     * @param s
     * @return
     * @throws DecoderException 
     */
    public static int stringToUnsignedIntegerCode(String s) {
        String md5 = DigestUtils.md5Hex(s);
        String md5Truncated = md5.substring(26, md5.length());

        try {
            Hex.decodeHex(md5Truncated.toCharArray());
        } catch (DecoderException e) {
            throw new RuntimeException("Error decoding string value: " + s, e);
        }
        //System.out.println("size: " + b.length);
        //System.out.println("" + md5 + " " + md5Truncated);
        int n = Integer.parseInt(md5Truncated, 16);
        
        return n;
    }

    public static long hexToLong(String s) {
        return hexToLong(s.getBytes());
    }

    public static long hexToLong(byte[] bytes) {

        if (bytes.length > 16) {
            throw new IllegalArgumentException(
                    "Byte array too long (max 16 elements)");
        }
        long v = 0;
        for (int i = 0; i < bytes.length; i += 2) {
            byte b1 = (byte) (bytes[i] & 0xFF);

            b1 -= 48;
            if (b1 > 9)
                b1 -= 39;

            if (b1 < 0 || b1 > 15) {
                throw new IllegalArgumentException("Illegal hex value: "
                        + bytes[i]);
            }

            b1 <<= 4;

            byte b2 = (byte) (bytes[i + 1] & 0xFF);
            b2 -= 48;
            if (b2 > 9)
                b2 -= 39;

            if (b2 < 0 || b2 > 15) {
                throw new IllegalArgumentException("Illegal hex value: "
                        + bytes[i + 1]);
            }

            v |= (((b1 & 0xF0) | (b2 & 0x0F))) & 0x00000000000000FFL;

            if (i + 2 < bytes.length)
                v <<= 8;
        }

        return v;
    }

}
