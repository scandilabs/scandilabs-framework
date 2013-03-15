package org.catamarancode.mail.send;

import java.io.File;

import javax.mail.internet.MimeMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.mail.javamail.MimeMessagePreparator;

/**
 * A simple wrapper around Spring's JavaMailSender which in turn wraps java's
 * JavaMail sender. This class makes it very simple to construct a basic
 * html-formatted email message, optionally with an attachment included.
 * 
 * @author mkvalsvik, bones
 * 
 */
public class SimpleHtmlMailSender {

    private static Logger logger = LoggerFactory
            .getLogger(SimpleHtmlMailSender.class);

    private JavaMailSender mailSender;
    private int multiPartMode = MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED;
    private String encoding = "UTF-8";
    private String defaultTo;
    private String defaultCc;
    private String defaultBcc;
    private String defaultFrom;
    private String defaultSubject;
    
    /**
     * Whether to send all outbound mail to defaultTo, even if a specific To is specified
     */
    private boolean testMode = true;

    public SimpleHtmlMailSender(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    /**
     * No-attachment version
     * 
     * @param simpleMailMessage
     */
    public void send(final SimpleMailMessage simpleMailMessage) {
        this.send(simpleMailMessage, null);
    }

    /**
     * Uses a wrapped instance of JavaMailSender to send this message as a
     * MimeMessage, offering support for HTML text content, inline elements, etc
     */
    public void send(final SimpleMailMessage simpleMailMessage,
            final File attachment) {

        // Show only first recipient in log messages
        String firstRecipient = null;
        if (simpleMailMessage.getTo().length > 0) {
            firstRecipient = simpleMailMessage.getTo()[0];
        }
        
        // Test mode?  If so then redirect message to the default recipient
        if (testMode && simpleMailMessage.getTo() != null) {
            
            logger.info(String.format("EMAIL TEST MODE: Message with subject %s will be sent to %s, not to %s", simpleMailMessage.getSubject(), this.getDefaultTo(), firstRecipient));

            // Clear to so that defaultTo will be used below. Note we have to use an empty array because setTo(null) won't work
            simpleMailMessage.setTo(new String[] {});
            firstRecipient = this.getDefaultTo();
        }
        
        MimeMessagePreparator preparator = new MimeMessagePreparator() {
            public void prepare(MimeMessage mimeMessage) throws Exception {
                MimeMessageHelper helper = new MimeMessageHelper(mimeMessage,
                        getMultiPartMode(), getEncoding());
                
                if (simpleMailMessage.getFrom() != null) {
                    helper.setFrom(simpleMailMessage.getFrom());    
                } else if (getDefaultFrom() != null) {
                    helper.setFrom(getDefaultFrom());
                }
                
                if (simpleMailMessage.getTo() != null && simpleMailMessage.getTo().length > 0) {
                    helper.setTo(simpleMailMessage.getTo());    
                } else if (getDefaultTo() != null) {
                    helper.setTo(getDefaultTo());
                }
                
                if (simpleMailMessage.getCc() != null) {
                    helper.setCc(simpleMailMessage.getCc());    
                } else if (getDefaultCc() != null) {
                    helper.setCc(getDefaultCc());
                }
                
                if (simpleMailMessage.getBcc() != null) {
                    helper.setBcc(simpleMailMessage.getBcc());    
                } else if (getDefaultBcc() != null) {
                    helper.setBcc(getDefaultBcc());
                }
                
                if (simpleMailMessage.getSubject() != null) {
                    helper.setSubject(simpleMailMessage.getSubject());    
                } else if (getDefaultSubject() != null) {
                    helper.setSubject(getDefaultSubject());
                }
                
                helper.setText(simpleMailMessage.getText(), true); // true means
                // enable
                // html email

                // Add an attachment
                // See
                // (http://static.springframework.org/spring/docs/2.0.x/reference/mail.html)
                if (attachment != null) {
                    FileSystemResource file = new FileSystemResource(attachment);
                    helper.addAttachment(attachment.getName(), file);
                }

            }
        };
        JavaMailSenderImpl senderImpl = (JavaMailSenderImpl) this.mailSender;

        logger.info(String.format("Sending mail to %s, subject: %s", firstRecipient,
                simpleMailMessage.getSubject()));
        
        // Call JavaMail to send
        this.mailSender.send(preparator);
    }

    /**
     * @return the desired multipart mode for the email to be sent. Defaults to
     *         MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED;
     */
    public int getMultiPartMode() {
        return multiPartMode;
    }

    /**
     * Defaults to MimeMessageHelper.MULTIPART_MODE_MIXED_RELATED, so this
     * should not need to be set. Added just in case...
     * 
     * @param multiPartMode
     */
    public void setMultiPartMode(int multiPartMode) {
        this.multiPartMode = multiPartMode;
    }

    /**
     * @return the desired encoding type. Defaults to UTF-8
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Defaults to UTF-8, so this should not need to be set. Added just in
     * case...
     * 
     * @param encoding
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setMailSender(JavaMailSender mailSender) {
        this.mailSender = mailSender;
    }

    public void setDefaultTo(String defaultTo) {
        this.defaultTo = defaultTo;
    }

    public void setDefaultCc(String defaultCc) {
        this.defaultCc = defaultCc;
    }

    public void setDefaultFrom(String defaultFrom) {
        this.defaultFrom = defaultFrom;
    }

    public void setDefaultSubject(String defaultSubject) {
        this.defaultSubject = defaultSubject;
    }

    public String getDefaultTo() {
        return defaultTo;
    }

    public String getDefaultCc() {
        return defaultCc;
    }

    public String getDefaultFrom() {
        return defaultFrom;
    }

    public String getDefaultSubject() {
        return defaultSubject;
    }

    public void setTestMode(boolean testMode) {
        this.testMode = testMode;
    }

    public boolean isTestMode() {
        return testMode;
    }

    public String getDefaultBcc() {
        return defaultBcc;
    }

    public void setDefaultBcc(String defaultBcc) {
        this.defaultBcc = defaultBcc;
    }
}
