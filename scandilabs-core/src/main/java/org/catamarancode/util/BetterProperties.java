package org.catamarancode.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Enhancements to java.util.Properties
 * @author mkvalsvik
 *
 */
public class BetterProperties extends Properties {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public BetterProperties() {
        super();
    }
    
    public BetterProperties(Properties defaults) {
        super(defaults);
    }
    
    public int getPropertyInt(String key) {
        String value = this.getProperty(key);
        return Integer.parseInt(value);    
    }
    
    public static BetterProperties loadFromFile(String absoluteFileName) {
        BetterProperties properties = new BetterProperties();
        File file = new File(
                absoluteFileName);
        if (!file.exists()) {
            throw new RuntimeException(
                    "Missing required properties file: " + absoluteFileName);
        }
        try {
            properties.load(new FileInputStream(file));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error loading properties file: " + absoluteFileName,
                    e);
        }
        return properties;
    }
}
