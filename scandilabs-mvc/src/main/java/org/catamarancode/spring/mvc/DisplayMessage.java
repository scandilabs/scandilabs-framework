package org.catamarancode.spring.mvc;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.servlet.ModelAndView;

/**
 * Helper class for storing and displaying page-level messages on Spring MVC
 * view pages. Typically, a controller that forwards to it's own view will use
 * addToView or addToModel. If a controller may receive redirects from other
 * controllers then it should also use addPendingToView or addPendingToModel to
 * display messages from the previous controller. A controller that ends by
 * redirecting to another path should use addPending when adding a message for
 * display.
 * 
 * Note that this is NOT inherently a messageing model that is well suited for
 * multi-lingual sites. Think of it as something better than no message
 * framework at all, but less flexible than Spring MVC's built-in
 * messages.properties based framework.
 * 
 * 
 * @author mkvalsvik
 * 
 */
public class DisplayMessage {

    private boolean success;
    private String text;

    public static final String PAGE_VIEW_KEY = "message";
    public static final String SESSION_KEY = "DisplayMessage_displayMessage";

    /**
     * Show any previously added messages by adding them to the given Model
     * 
     * @param request
     * @param mv
     */
    public static void addPendingToThisPage(HttpServletRequest request,
            Map<String, Object> model) {
        DisplayMessage sessionMessage = (DisplayMessage) request.getSession()
                .getAttribute(SESSION_KEY);
        if (sessionMessage != null) {
            addToThisPage(model, sessionMessage);
            request.getSession().setAttribute(SESSION_KEY, null);
        }
    }

    /**
     * Add a message to be shown next time we display a View
     * 
     * @param request
     * @param message
     */
    public static void addToNextPage(HttpServletRequest request,
            DisplayMessage message) {
        request.getSession().setAttribute(SESSION_KEY, message);
    }

    /**
     * Add a message to be shown next time we display a View
     * 
     * @param request
     * @param message
     * @param success
     */
    public static void addToNextPage(HttpServletRequest request,
            String message, boolean success) {
        request.getSession().setAttribute(SESSION_KEY,
                new DisplayMessage(message, success));
    }

    public static void addToThisPage(Map<String, Object> model,
            DisplayMessage message) {
        model.put(PAGE_VIEW_KEY, message);
    }

    public static void addToThisPage(Map<String, Object> model, String message,
            boolean success) {
        addToThisPage(model, new DisplayMessage(message, success));
    }

    public static void addToThisPage(ModelAndView mv, DisplayMessage message) {
        addToThisPage(mv.getModelMap(), message);
    }

    public static void addToThisPage(ModelAndView mv, String message,
            boolean success) {
        addToThisPage(mv.getModelMap(), message, success);
    }

    public DisplayMessage(String text, boolean success) {
        this.success = success;
        this.text = text;
    }

    /**
     * Add a message to be shown next time we display a View
     * 
     * @param request
     */
    public void addToNextPage(HttpServletRequest request) {
        addToNextPage(request, this);
    }

    public String getSuccessString() {
        String prefix = "error";
        if (success) {
            prefix = "success";
        }
        return prefix;
    }

    public String getText() {
        return text;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setTest(String text) {
        this.text = text;
    }

    public String toString() {
        return String.format("%s: %s", this.getSuccessString(), text);
    }

}
