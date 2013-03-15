package org.catamarancode.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * Utilities for java.net.URI
 * 
 * @author mkvalsvik
 * 
 */
public class URIUtils {

    /**
     * Tester method
     * 
     * @param args
     */
    public static void main(String[] args) {
        try {
            URI uri = new URI("http", null, "www.postpo.st", -1, "/search",
                    "q=silverton", "mysilvertonpostpost");
            System.out.println(uri);
            System.out.println(stripSubdomain(uri));

            uri = new URI("http://localhost/search?q=silverton#mypopo");
            System.out.println(uri);
            System.out.println(stripSubdomain(uri));

            uri = new URI("http://localhost:8080/search?q=silverton#mypopo");
            System.out.println(uri);
            System.out.println(stripSubdomain(uri));

        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private URIUtils() {
    }

    public static boolean hasSubDomain(String hostName) {
        int firstDotPos = hostName.indexOf(".");
        int lastDotPos = hostName.lastIndexOf(".");
        if (firstDotPos == -1) {

            // No dot
            return false;
        }
        if (firstDotPos == lastDotPos) {

            // Found only one dot
            return false;
        }

        // Multiple dots
        return true;
    }

    public static String stripSubdomain(String hostName) {
        if (hasSubDomain(hostName)) {
            int firstDotPos = hostName.indexOf(".");
            return hostName.substring(firstDotPos + 1);
        } else {
            return hostName;
        }
    }

    public static URI stripSubdomain(URI uri) {
        String hostName = uri.getHost();
        int port = -1;
        if (uri.getPort() != -1 && uri.getPort() != 80) {
            port = uri.getPort();
        }
        URI returnURI;
        try {
            returnURI = new URI(uri.getScheme(), null,
                    stripSubdomain(hostName), port, uri.getPath(), uri
                            .getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Problems with uri %s",
                    uri.toString()), e);
        }
        return returnURI;
    }

    public static String urlEncodeUTF8(String s) {
        try {
            return URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedOperationException(e);
        }
    }

    /**
     * Determine the absolute context path from an absoluteURL and a relative
     * context path. Useful for <base href='.. tags and absolute url prefixes to
     * html links and includes.
     * 
     * @param absoluteURL
     *            an absolute URL of the format [protocol]://[host]:[optional
     *            port]/contextPath. The contextPath is optional (i.e. ROOT
     *            context is ok). Will typically be obtained from
     *            request.getRequestURL().toString() where request is an
     *            HttpServletRequest
     * @param contextPath
     *            a context path that may start with a slash. Typically obtained
     *            from request.getContextPath() where request is an
     *            HttpServletRequest. May be empty string "" in which case it
     *            will be assumed that the root context is being used.
     * @return an absolute path that looks like either http://host.domain.com,
     *         https://host.domain.com, or http://host.domain.com/context
     */
    public static String toAbsoluteContextPath(String absoluteURL,
            String contextPath) {
        // skip http:// or https://
        int firstSlashPos = absoluteURL.substring(8).indexOf("/") + 8;
        String hostAndContext = null;
        if (contextPath.equals("")) {
            hostAndContext = absoluteURL.substring(0, firstSlashPos);
        } else {
            int contextEndSlashPos = absoluteURL.substring(firstSlashPos + 1)
                    .indexOf("/");
            hostAndContext = absoluteURL.substring(0, firstSlashPos + 1
                    + contextEndSlashPos);
        }
        return hostAndContext;
    }

    public static String toQueryString(Map<?, ?> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sb.length() > 0) {
                sb.append("&");
            } else {
                sb.append("?");
            }
            Object value = entry.getValue();
            if (entry.getValue() instanceof Object[]) {

                // If value is an array, take first element only
                Object[] values = (Object[]) entry.getValue();
                value = values[0];
            }
            sb.append(String.format("%s=%s", urlEncodeUTF8(entry.getKey()
                    .toString()), urlEncodeUTF8(value.toString())));
        }
        return sb.toString();
    }

}
