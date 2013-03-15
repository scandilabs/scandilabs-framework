package org.catamarancode.monitor.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;

/**
 * Watches all log files in a given directory and sends email when an exception
 * condition is detected. Simple string inspection via a configured regex.
 * 
 * @author mkvalsvik
 * 
 */
public class LogMonitor {

    private String smtpHost = "smtp.gmail.com";
    private int smtpPort = 465;
    private String smtpProtocol = "smtps";
    private String smtpUsername = null;
    private String smtpPassword = null;
    private String smtpSslAuth = "true";
    private String smtpSslStartTls = "true";
    private String smtpSslDebug = "true";
    private String smtpFrom = null;
    private String smtpTo = null;
    private String smtpSubject = null;
    private static final String[] FILE_PREFIXES_TO_SKIP = new String[] {"log-monitor", "catamaran", "nohup" };

    /**
     * 1MB max
     */
    private static final long MAX_MESSAGE_SIZE = 1024 * 1024;

    private static final long POLLING_DELAY_IN_MILLIS = 1000;

    private Pattern searchPattern = null;

    private final Calendar calendar = new GregorianCalendar();

    private long lastMessageSendTime = 0;

    private boolean sendEveryMinute = false;

    File directoryToWatch = null;
    
    File fileToWatch = null;

    private List<WatchedFile> watchedFiles = new ArrayList<WatchedFile>();

    private Properties properties = new Properties();

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd hh:mm:ss,S");

    private static void log(String line) {
        System.out.print(dateFormat.format(new Date()));
        System.out.print(" ");
        System.out.println(line);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {

        // Initialize
        LogMonitor monitor = new LogMonitor();
        monitor.init();

        // Watching single file?
        if (monitor.fileToWatch != null) {
            monitor.watchFile(monitor.fileToWatch);
            monitor.runWatcherDaemon(false);
        } else {
         
            // Watch entire directory
            monitor.refreshWatchedFiles();
            monitor.runWatcherDaemon(true);
        }
    }

    private static void searchFile(WatchedFile watchedFile, 
            Pattern searchPattern, StringBuilder messageBuffer)
            throws IOException {
        String line = null;
        do {
            line = watchedFile.getReader().readLine();
            if (line != null) {
                if (searchLine(searchPattern, line) != null) {

                    // Beware that the messageBuffer may fill up. If
                    // it's full then wipe it clean. This is based
                    // on the assumption that if there are multiple
                    // monitor entry hits then the most recent ones
                    // will be the most interesting anyway.
                    if (messageBuffer.length() > MAX_MESSAGE_SIZE) {
                        log("WARN: Message size max (%d) exceeded, wiping message buffer");
                        messageBuffer.delete(0, messageBuffer.length());
                    }
                    messageBuffer.append(String.format("%s: %s",
                            watchedFile.getFile().getName(), line));
                } else {
                    /*
                     * System.out.println(String.format( "Nothing in %s: %s",
                     * logFiles[i] .getName(), line));
                     */
                }
            }
        } while (line != null);
    }

    private static String searchLine(Pattern searchPattern, String line) {
        Matcher m = searchPattern.matcher(line);
        if (m.matches()) {
            return line;
        }
        return null;
    }

    private StringBuilder messageBuffer = new StringBuilder();

    public void init() throws IOException {

        // Load properties
        File propertiesFile = new File("log-monitor.properties");
        if (!propertiesFile.exists()) {
            System.err
                    .println("Properties file not found: log-monitor.properties");
            System.exit(0);
        }
        properties.load(new FileInputStream(propertiesFile));

        // Initialize regex pattern
        String patternString = properties.getProperty("log.entry.token.regex");
        searchPattern = Pattern.compile(patternString);

        // Set directory to watch
        String directoryPath = properties.getProperty("log.directory.to.watch");
        if (directoryPath == null) {

            // Default to current directory
            directoryPath = new File(".").getAbsolutePath();
        }
        directoryToWatch = new File(directoryPath);
        if (directoryToWatch.exists() && directoryToWatch.isDirectory()) {
            log("INFO: Starting in directory: "
                    + directoryToWatch.getAbsolutePath() + ", looking for: "
                    + searchPattern.pattern());
        } else {
            System.err.println("Invalid directory: " + directoryPath);
            return;
        }
        
        // Set specific file to watch (overrides directory)
        String filePath = properties.getProperty("log.file.to.watch");
        if (filePath != null) {
            fileToWatch = new File(filePath);
        }

        // Send every minute or every hour?
        String minutePropertyString = properties
                .getProperty("log.send.every.minute");
        if (minutePropertyString != null
                && Boolean.parseBoolean(minutePropertyString)) {
            this.sendEveryMinute = true;
        }

        // Email setup
        String host = properties.getProperty("email.host");
        if (host != null) {
            this.smtpHost = host;
        }
        String portString = properties.getProperty("email.port");
        if (portString != null) {
            this.smtpPort = Integer.parseInt(portString);
        }
        String protocol = properties.getProperty("email.protocol");
        if (protocol != null) {
            this.smtpProtocol = protocol;
        }
        this.smtpUsername = properties.getProperty("email.username");
        this.smtpPassword = properties.getProperty("email.password");
        String auth = properties.getProperty("email.mail.smtps.auth");
        if (auth != null) {
            this.smtpSslAuth = auth;
        }
        String tls = properties.getProperty("email.mail.smtps.starttls.enable");
        if (tls == null) {
            this.smtpSslStartTls = tls;
        }
        String debug = properties.getProperty("email.mail.smtps.debug");
        if (debug != null) {
            this.smtpSslDebug = debug;
        }
        this.smtpFrom = properties.getProperty("email.default.from");
        this.smtpTo = properties.getProperty("email.to");
        this.smtpSubject = properties.getProperty("email.subject");
    }

    /**
     * Is it time to send message?
     * 
     * @return
     */
    private boolean isTimeToSendMessage() {
        calendar.setTimeInMillis(this.lastMessageSendTime);
        int hourOrMinuteField = -1;
        if (this.sendEveryMinute) {

            // Send every minute
            hourOrMinuteField = Calendar.MINUTE;
        } else {

            // Send every hour
            hourOrMinuteField = Calendar.HOUR_OF_DAY;
        }

        // Crude but works: Look for a change in the minute or hour field
        int lastTime = calendar.get(hourOrMinuteField);
        calendar.setTimeInMillis(System.currentTimeMillis());
        int thisTime = calendar.get(hourOrMinuteField);
        if (thisTime > lastTime) {
            return true;
        }

        // Started a new day or hour?
        if (thisTime == 0 && lastTime > 0) {
            return true;
        }

        // Not time.
        return false;
    }
    
    public void watchFile(File file) throws IOException {
        log(String.format("INFO: Adding   %s", file.getName()));
         WatchedFile watchedFile = new WatchedFile(file);
         this.watchedFiles.add(watchedFile);
         
         // Skip until the end of the file
         String line = null;
         do {
             line = watchedFile.getReader().readLine();
         } while (line != null);                                             
    }

    public void refreshWatchedFiles() throws IOException {
        
        boolean changed = false;
        File[] files = this.directoryToWatch.listFiles();

        // Open all files and scroll till the end
        for (int i = 0; i < files.length; i++) {
            if (files[i].exists() && files[i].isFile()) {
                
                // TODO: Filter out file names with old dates in them
                
                // Filter out this service's own log file
                for (int j = 0; j < FILE_PREFIXES_TO_SKIP.length; j++) {
                    if (files[i].getName().startsWith(FILE_PREFIXES_TO_SKIP[j])) {
                        continue;
                    }
                }
                
                // Skip files that have already been added
                boolean alreadyAdded = false;
                for (WatchedFile stored : watchedFiles) {                    
                    if (stored.getFile().getName().equals(files[i].getName())) {
                        alreadyAdded = true;
                    }
                }
                if (alreadyAdded) {
                    continue;
                }

                // Add to watch
                this.watchFile(files[i]);
                changed = true;
            } 
        }
        
        if (changed) {
            log(String.format("INFO: Now watching %d files...", watchedFiles.size()));    
        }        
    }

    /**
     * Runs in an infinite loop
     * @param addNewFiles
     * @throws IOException
     */
    public void runWatcherDaemon(boolean addNewFiles) throws IOException {        
        while (true) {

            // Loop over all watched files
            for (int i = 0; i < this.watchedFiles.size(); i++) {
                if (this.watchedFiles.get(i) != null) {
                    searchFile(this.watchedFiles.get(i), this.searchPattern,
                            this.messageBuffer);
                }
            }

            // Is it time to send message?
            if (isTimeToSendMessage()) {

                // Send
                this.lastMessageSendTime = calendar.getTimeInMillis();
                this.sendMessageFromBuffer();

                // Also refresh file readers in case a file has been added to
                // the watched directory
                if (addNewFiles) {
                    refreshWatchedFiles();
                }
            }

            // File polling delay
            try {
                Thread.sleep(POLLING_DELAY_IN_MILLIS);
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }
    }

    private void sendMessageFromBuffer() {
        if (this.messageBuffer.length() > 0) {
            log(String.format("INFO: Sending %d characters", this.messageBuffer
                    .length()));

            // Initialize sender
            JavaMailSenderImpl sender = new JavaMailSenderImpl();
            sender.setHost(this.smtpHost);
            sender.setPort(this.smtpPort);
            sender.setProtocol(this.smtpProtocol);
            sender.setUsername(this.smtpUsername);
            sender.setPassword(this.smtpPassword);
            Properties props = new Properties();
            props.setProperty("mail.smtps.auth", this.smtpSslAuth);
            props.setProperty("mail.smtps.starttls.enable",
                    this.smtpSslStartTls);
            props.setProperty("mail.smtps.debug", this.smtpSslDebug);
            sender.setJavaMailProperties(props);

            // Create and send message
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(this.smtpFrom);
            message.setTo(this.smtpTo);
            message.setSubject(this.smtpSubject);
            message.setText(this.messageBuffer.toString());
            sender.send(message);
            log(String.format(
                    "INFO: Sent message with subject %s to %s", message.getSubject(),
                    message.getTo()[0]));

            // Clear the buffer
            this.messageBuffer.delete(0, this.messageBuffer.length());
        }
    }

}
