package org.catamarancode.mail.compose.freemarker;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.springframework.mail.SimpleMailMessage;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Populates the body text portion of a spring SimpleMailMessage by merging a
 * freemarker template with a data model
 * 
 * @author mkvalsvik
 * 
 */
public class FreemarkerSimpleMessageBodyComposer {

    public FreemarkerSimpleMessageBodyComposer(
            Configuration freemarkerConfiguration) {
        this.freemarkerConfiguration = freemarkerConfiguration;
    }

    private Configuration freemarkerConfiguration;

    /**
     * Populates the body (also called 'text') field of a email message from a
     * freemarker template and data model
     * 
     * @param message
     *            The message whose body/text field to populate
     * @param bodyTemplateFileName
     *            The file name (typically 'something.ftl') of the freemarker
     *            template. File name is relative to the root file path defined
     *            in freemarkerConfiguration. Sub-directories are ok (i.e.
     *            'customer/thank-you.ftl').
     * @param model
     *            The data which will be substituted into the freemarker
     *            template's ${object.propertyName} placeholder tags
     * @return a unique identifier of this email message that can be used as a
     *         key for later lookups to this email (for instance, by clicking on
     *         a link to an online version of the email)
     * 
     * @throws RuntimeException
     *             if anything goes wrong
     */
    public String populateMessageBody(SimpleMailMessage message,
            String bodyTemplateFileName, Map<String, Object> model) {

        // Generate a unique email id that can be used for linking to online
        // versions of this email
        String onlineId = RandomStringUtils.randomAlphanumeric(8);
        model.put("onlineEmailId", onlineId);

        StringWriter writer = null;
        Template template;
        try {
            template = freemarkerConfiguration
                    .getTemplate(bodyTemplateFileName);
            writer = new StringWriter();
            template.process(model, writer);
            message.setText(writer.toString());
        } catch (IOException e) {
            throw new RuntimeException("IO error related to template file "
                    + bodyTemplateFileName, e);
        } catch (TemplateException e) {
            throw new RuntimeException(
                    "Improperly formatted email body template file "
                            + bodyTemplateFileName, e);
        }

        // Return the online message id so that it can be persisted for later
        // lookup. Note that clients that don't persist emails locally can safely ignore this value.
        return onlineId;
    }

    public void setFreemarkerConfiguration(Configuration freemarkerConfiguration) {
        this.freemarkerConfiguration = freemarkerConfiguration;
    }
}
