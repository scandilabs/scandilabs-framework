package org.catamarancode.mail.client;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSenderImpl;

/**
 * A command-line utility for sending e-mail via Gmail SMTP
 * @author mkvalsvik
 *
 */
public class GmailSender {

    private static Logger logger = LoggerFactory.getLogger(GmailSender.class);
    
    public static void main(String[] args) {

        // Validate
        if (args.length < 6) {
            System.out.println("Usage: java GmailSender [gmail_username] [gmail_password] [from] [to] [subject] [body]");
            System.exit(1);
        }
        
        // Initialize sender
        JavaMailSenderImpl sender = new JavaMailSenderImpl();
        sender.setHost("smtp.gmail.com");
        sender.setPort(465);
        sender.setProtocol("smtps");
        sender.setUsername(args[0]);
        sender.setPassword(args[1]);
        Properties props = new Properties();
        props.setProperty("mail.smtps.auth", "true");
        props.setProperty("mail.smtps.starttls.enable", "true");
        props.setProperty("mail.smtps.debug", "true");
        sender.setJavaMailProperties(props);

        // Create and send message
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(args[2]);
        message.setTo(args[3]);
        message.setSubject(args[4]);
        message.setText(args[5]);        
        sender.send(message);
        System.out.println(String.format("Sent message with subject %s to %s", message.getSubject(), message.getTo()[0]));
        
    }
    
}
