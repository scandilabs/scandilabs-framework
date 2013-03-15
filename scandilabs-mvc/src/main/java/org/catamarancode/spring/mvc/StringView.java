package org.catamarancode.spring.mvc;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.web.servlet.view.AbstractView;

/**
 * A spring MVC view class that takes the first object it finds in the model and
 * uses toString() to render it without any templating or formatting
 * 
 * @author mkvalsvik
 * 
 */
public class StringView extends AbstractView {

    private Logger logger = LoggerFactory.getLogger(StringView.class);
    
    private boolean enableDebugLog = false;
    
    @Override
    protected void renderMergedOutputModel(Map model,
            HttpServletRequest request, HttpServletResponse response)
            throws Exception {

        if (model.isEmpty()) {
            throw new RuntimeException(
                    "No model object found, view cannot render anything");
        }

        // Find the first object in the model, excluding Spring MVC framework-specific objects
        Object displayObject = null;
        for (Object obj : model.values()) {
            if (obj instanceof BeanPropertyBindingResult) {
                // skip
            } else {
                displayObject = obj;
                break;
            }
        }
        
        String s = displayObject.toString();
        
        if (enableDebugLog) {
            logger.debug(String.format("%s result: %s", request.getRequestURI(), s));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(s.getBytes());
        writeToResponse(response, baos);
    }

    public void setEnableDebugLog(boolean enableDebugLog) {
        this.enableDebugLog = enableDebugLog;
    }

}
