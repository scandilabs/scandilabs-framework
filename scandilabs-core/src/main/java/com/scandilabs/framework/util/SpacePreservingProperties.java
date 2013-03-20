package com.scandilabs.framework.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import com.scandilabs.framework.util.BetterProperties;

/**
 * Properties implementation that respects spaces in property key names
 * 
 * @author mkvalsvik
 * 
 */
public class SpacePreservingProperties extends BetterProperties {

    private static final long serialVersionUID = 1L;

    public SpacePreservingProperties() {
        super();
    }
    
    public SpacePreservingProperties(Properties defaults) {
        super(defaults);
    }
    
    /**
     * Override the default load method from java.util.Properties
     */
    public void load(Reader reader) {
        BufferedReader r = new BufferedReader(reader);
        this.load(r);
    }
    
    public void load(BufferedReader reader) {
        try {
            String line = reader.readLine();
            while (line != null) {
                int equalPos = line.indexOf('=');
                if (equalPos < 1) {
                    // Skip
                } else {
                    String key = line.substring(0, equalPos);
                    String value = line.substring(equalPos+1);
                    this.setProperty(key, value);
                }
                line = reader.readLine();
            }

        } catch (IOException e) {
            throw new RuntimeException("Error reading properties", e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
    
    /**
     * Override the default load method from java.util.Properties
     */
    public void load(InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));        
        this.load(reader);
    }
}
