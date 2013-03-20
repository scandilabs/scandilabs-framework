package com.scandilabs.framework.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple security scheme for authenticated services. A client is issued with
 * a key (not protected, can be passed in a URL) and a secret. The client uses
 * buildToken() to create a token, and the key and the token are passed to a
 * server. The server verifies the token using a private keystore of all valid
 * key/secret pairs.
 * 
 * @author mkvalsvik
 * 
 */
public class SharedSecretNoSaltHelper {
    
    private Logger logger = LoggerFactory.getLogger(SharedSecretNoSaltHelper.class);

    /**
     * A secret that is shared between the client and server
     */
    private String serviceSecret;

    /**
     * Keys mapped to a key-specific secret. They key can be passed from client
     * to server, but the secret never is
     */
    private Properties keyToSecretMappings;

    public SharedSecretNoSaltHelper(Properties keyToSecretMappings) {
        this(keyToSecretMappings, null);
    }

    public SharedSecretNoSaltHelper(Properties keyToSecretMappings,
            String serviceSecret) {
        this.keyToSecretMappings = keyToSecretMappings;
        this.serviceSecret = serviceSecret;
    }

    private static final String DEFAULT_SERVICE_SECRET = "c575a84b62c5271fe71ba754c42bc882";

    public boolean verifyClientToken(String key, String clientToken) {

        // Compute a comparison server token with this key and the secret we
        // have stored
        String secret = keyToSecretMappings.getProperty(key);
        Calendar calendar = Calendar.getInstance();
        String serverToken = buildToken(key, secret, this.serviceSecret, calendar.getTime());

        // Compare
        boolean match = serverToken.equals(clientToken);
        logger.debug(String.format("Compared client token %s to %s", clientToken, serverToken));
        
        if (!match) {
             
            // To allow for calling delays, allow a 1-minute old timestamp as well
            calendar.add(Calendar.MINUTE, -1);
            serverToken = buildToken(key, secret, this.serviceSecret, calendar.getTime());
            match = serverToken.equals(clientToken);
            logger.debug(String.format("2nd compare of client token %s to %s", clientToken, serverToken));
        }
        return match;
    }
    
    public static String buildToken(String key, String secret) {
        return buildToken(key, secret, null);
    }
    
    private static DateFormat minuteTimestampFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");

    public static String buildToken(String key, String secret, String serviceSecret) {
        return buildToken(key, secret, serviceSecret, new Date());        
    }
    
    private static String buildToken(String key, String secret, String serviceSecret, Date currentTime) {
        
        // Start with keys and secrets
        StringBuffer sb = new StringBuffer();
        if (serviceSecret != null) {
            sb.append(serviceSecret);    
        } else {
            sb.append(DEFAULT_SERVICE_SECRET);
        }        
        sb.append(key);
        sb.append(secret);
        
        // Add a UTC timestamp accurate to the minute. Verifying code must remember to check timestamp minus 1 minute as well
        minuteTimestampFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String timestamp = minuteTimestampFormat.format(currentTime);
        sb.append(timestamp);
        
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(
                    "Password encoding algorithm SHA-1 not found, please check you Java version",
                    e);
        }
        try {
            md.update(sb.toString().getBytes("UTF-8"));
            byte[] hash = md.digest();
            Base64 base64Encoder = new Base64();
            byte[] base64EncodedHash = base64Encoder.encode(hash);
            return new String(base64EncodedHash, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                    "UTF-8 not supported, please check you Java version", e);
        }
    }
}
