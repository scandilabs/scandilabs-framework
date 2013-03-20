package com.scandilabs.framework.mvc;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.catamarancode.util.URIUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

/**
 * An exception resolver that either displays an html page with stack trace with
 * a unique reference code or a Json error.
 * 
 * jQuery and similar javascript frameworks will insert a header element into
 * every http request which we can use to determine whether a request is an
 * ajax/json call or a regular browser call. We will use this information to
 * decide whether to return an error message as JSON or as an html page.
 * 
 * This resolver will also pay special attention to instances of
 * UserNotSignedInException, and show the signin view if it is determined that
 * the user has not signed in
 */
public class HtmlAndJsonExceptionResolver implements HandlerExceptionResolver {

    private Logger logger = LoggerFactory
            .getLogger(HtmlAndJsonExceptionResolver.class);

    private String htmlErrorView = "error";
    private String jsonErrorView = "jsonError";
    private boolean productionEnvironment = false;
    private String targetPathSessionKey = "targetPath";
    private String ajaxRequestIdentificationHeader = "X-Requested-With";
    private String signinViewName = "signin";
    private String signinFailureMessage = "Please sign in first";
    
    /**
     * Whether to forward (default) or redirect to the signing view on authentication failure
     */
    private boolean redirectOnSigninFailure = false;

    private boolean isProduction() {
        return productionEnvironment;
    }

    public ModelAndView resolveException(HttpServletRequest request,
            HttpServletResponse response, Object handler, Exception ex) {

        // Generate unique error code
        String reference = RandomStringUtils.randomAlphanumeric(10)
                .toUpperCase();

        ModelAndView mv = null;

        boolean isAjaxRequest = StringUtils.hasText(request
                .getHeader(ajaxRequestIdentificationHeader));
        if (isAjaxRequest) {

            // This is an ajax or json request, therefore we render with the
            // default "string" view
            mv = new ModelAndView(jsonErrorView);
            mv.addObject("responseCode", 0);
            
        } else {

            // This is a regular browser http requst. So we render error as html
            mv = new ModelAndView(htmlErrorView);

            // Treat not logged in exception differently
            if (ex instanceof UserNotSignedInException) {

                // Store target path in session
                String targetPath = request.getContextPath()
                        + request.getServletPath()
                        + (request.getPathInfo() != null ? request
                                .getPathInfo() : "")
                        + URIUtils.toQueryString(request.getParameterMap());
                request.getSession().setAttribute(targetPathSessionKey,
                        targetPath);

                if (redirectOnSigninFailure) {
                    mv = new ModelAndView("redirect:" + signinViewName);
                    DisplayMessage.addToNextPage(request, signinFailureMessage, false);
                } else {
                    mv = new ModelAndView(signinViewName);
                    DisplayMessage.addToThisPage(mv, signinFailureMessage, false);
                }
                return mv;
            }
        }

        // Useful error statistics
        mv.addObject("logExceptionTime", new Date());
        mv.addObject("logExceptionID", reference);

        // Include error code if known
        String errorMessage = ex.getMessage();
        mv.addObject("logExceptionMessage", errorMessage);
        DisplayMessage.addToThisPage(mv, errorMessage, false);

        // Only show stack trace if we're not in production mode
        if (!isProduction()) {
            String stack = ExceptionUtils.getFullStackTrace(ex).replaceAll(
                    "\n", "<br>");
            mv.addObject("logException", stack);
        }

        if (isAjaxRequest) {
            logger
                    .error(
                            String
                                    .format(
                                            "Handled error via JSON response to client, reference id %s: %s",
                                            reference, errorMessage), ex);
        } else {
            logger
                    .error(
                            String
                                    .format(
                                            "Handled error via HTML response to client, reference id %s: %s",
                                            reference, errorMessage), ex);
        }

        return mv;
    }

    public void setHtmlErrorView(String htmlErrorView) {
        this.htmlErrorView = htmlErrorView;
    }

    public void setJsonErrorView(String jsonErrorView) {
        this.jsonErrorView = jsonErrorView;
    }

    public void setProductionEnvironment(boolean live) {
        this.productionEnvironment = live;
    }
    
    public void setProductionEnvironmentString(String liveString) {
        this.productionEnvironment = Boolean.parseBoolean(liveString);
    }

    public String getTargetPathSessionKey() {
        return targetPathSessionKey;
    }

    public void setTargetPathSessionKey(String targetPathSessionKey) {
        this.targetPathSessionKey = targetPathSessionKey;
    }

    public String getHtmlErrorView() {
        return htmlErrorView;
    }

    public String getJsonErrorView() {
        return jsonErrorView;
    }

    public String getAjaxRequestIdentificationHeader() {
        return ajaxRequestIdentificationHeader;
    }

    public void setAjaxRequestIdentificationHeader(
            String ajaxRequestIdentificationHeader) {
        this.ajaxRequestIdentificationHeader = ajaxRequestIdentificationHeader;
    }

    public String getSigninViewName() {
        return signinViewName;
    }

    public void setSigninViewName(String signinViewName) {
        this.signinViewName = signinViewName;
    }

    public String getSigninFailureMessage() {
        return signinFailureMessage;
    }

    public void setSigninFailureMessage(String signinFailureMessage) {
        this.signinFailureMessage = signinFailureMessage;
    }

    public boolean isRedirectOnSigninFailure() {
        return redirectOnSigninFailure;
    }

    public void setRedirectOnSigninFailure(boolean redirectOnSigninFailure) {
        this.redirectOnSigninFailure = redirectOnSigninFailure;
    }

    public boolean isProductionEnvironment() {
        return productionEnvironment;
    }

}
