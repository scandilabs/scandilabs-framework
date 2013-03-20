package com.scandilabs.framework.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Helper methods related to executing java programs as UNIX-style daemons 
 * @author mkvalsvik
 *
 */
public class DaemonUtils {

    private DaemonUtils() {}
    
    public static void writePidToFile(String fileName) throws IOException {
        String pid = getPid();
        File pidFile = new File(fileName);
        FileWriter writer = new FileWriter(pidFile);
        writer.write(pid);
        writer.close();
    }
    
    public static void removePidFile(String fileName) throws IOException {
        File pidFile = new File(fileName);
        pidFile.delete();
    }
    
    public static String getPid() {
        File proc_self = new File("/proc/self");
        if(proc_self.exists()) try {
            return proc_self.getCanonicalFile().getName();
        }
        catch(Exception e) {
            /// Continue on fall-back
        }
        File bash = new File("/bin/bash");
        if(bash.exists()) {
            ProcessBuilder pb = new ProcessBuilder("/bin/bash","-c","echo $PPID");
            try {
                Process p = pb.start();
                BufferedReader rd = new BufferedReader(new InputStreamReader(p.getInputStream()));
                return rd.readLine();
            }
            catch(IOException e) {
                return String.valueOf(Thread.currentThread().getId());
            }
        }
        // This is a cop-out to return something when we don't have BASH
        return String.valueOf(Thread.currentThread().getId());
    }

}
