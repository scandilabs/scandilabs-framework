package org.catamarancode.monitor.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class WatchedFile {

    private BufferedReader reader;
    private File file;
    
    public WatchedFile(File file) throws IOException {
        this.file = file;
        reader = new BufferedReader(new FileReader(
                file));
    }

    public BufferedReader getReader() {
        return reader;
    }

    public File getFile() {
        return file;
    }
    
    public void close() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }
}
