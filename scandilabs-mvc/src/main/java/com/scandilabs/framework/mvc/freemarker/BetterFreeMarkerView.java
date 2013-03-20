package com.scandilabs.framework.mvc.freemarker;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.servlet.view.freemarker.FreeMarkerView;

import freemarker.template.SimpleHash;

/**
 * Miscellaneous convenience enhancements to the standard Spring freemarker view
 * support.
 * 
 * Enabled by inserting this property into the freemarkerViewResolver bean:
 * <property name="viewClass"
 * value="org.catamarancode.spring.mvc.freemarker.BetterFreeMarkerView" />
 * 
 * @author mkvalsvik
 * 
 */
public class BetterFreeMarkerView extends FreeMarkerView {

    /**
     * Allows for injection of custom http response headers into each view Note
     * there are better ways of setting cache headers, see
     * http://stackoverflow.com
     * /questions/1362930/how-do-you-set-cache-headers-in-spring-mvc
     */
    protected static Map<String, String> preInjectedResponseHeaders;

    /**
     * allows for injection of standard application-wide model objects into
     * every view via the spring mvc configuration file
     */
    protected static Map<String, Object> preInjectedViewModel;
    
    /**
     * Convenience method which encapsulates any global changes to the response
     * @param request
     * @param response
     */
    protected void addToServletResponse(HttpServletRequest request, HttpServletResponse response) {
        if (preInjectedResponseHeaders != null) {
            for (String key : preInjectedResponseHeaders.keySet()) {
                response.addHeader(key, preInjectedResponseHeaders.get(key));
            }
        }
    }

    /**
     * Builds a better template model that exposes convenient objects
     */
    protected SimpleHash buildTemplateModel(Map<String, Object> model,
            HttpServletRequest request, HttpServletResponse response) {
        SimpleHash newModel = super
                .buildTemplateModel(model, request, response);

        // Pre-injected global model objects
        if (preInjectedViewModel != null) {
            for (String key : preInjectedViewModel.keySet()) {
                newModel.put(key, preInjectedViewModel.get(key));
            }
        }

        // Requesting client path -- useful for <base href='...' > tags
        // /mywebapp/servlet/MyServlet/a/b;c=123?d=789
        // TODO: Consider using URIUtils from catamaran-core
        String scheme = request.getScheme(); // http
        newModel.put("requestingScheme", scheme);
        String serverName = request.getServerName(); // hostname.com
        newModel.put("requestingServerName", serverName);
        int serverPort = request.getServerPort(); // 80
        newModel.put("requestingServerPort", serverPort);
        String contextPath = request.getContextPath(); // /mywebapp
        newModel.put("requestingContextPath", contextPath);
        String servletPath = request.getServletPath(); // /servlet/MyServlet
        newModel.put("requestingServletPath", servletPath);
        String pathInfo = request.getPathInfo(); // /a/b;c=123
        newModel.put("requestingPathInfo", pathInfo);
        String queryString = request.getQueryString(); // d=789
        newModel.put("requestingQueryString", queryString);

        // Reconstruct original requesting URL
        String url = scheme + "://" + serverName + ":" + serverPort
                + contextPath + servletPath;
        if (pathInfo != null) {
            url += pathInfo;
        }
        if (queryString != null) {
            url += "?" + queryString;
        }
        newModel.put("requestingUrl", url);

        // Reconstruct base urls
        String baseUrl = scheme + "://" + serverName + contextPath;
        newModel.put("requestingBaseUrl", baseUrl);
        String baseUrlWithPort = scheme + "://" + serverName + ":" + serverPort
                + contextPath;
        newModel.put("requestingBaseUrlWithPort", baseUrlWithPort);
        String baseUrlRootContext = scheme + "://" + serverName + ":"
                + serverPort + contextPath;
        newModel.put("requestingBaseUrlRootContext", baseUrlRootContext);

        // Even though this does not have anything to do with the model, take this opportunity to add stuff to response headers
        addToServletResponse(request, response);
        
        return newModel;
    }

    /**
     * This static setter gets invoked from spring -- see spring-mvc-servlet.xml
     * 
     * @param service
     */
    public static void setPreInjectedViewModel(Map<String, Object> model) {
        preInjectedViewModel = model;
    }
    
    /**
     * This static setter gets invoked from spring -- see spring-mvc-servlet.xml
     * 
     * @param service
     */
    public static void setPreInjectedResponseHeaders(Map<String, String> keyValuePairs) {
        preInjectedResponseHeaders = keyValuePairs;
    }
}
