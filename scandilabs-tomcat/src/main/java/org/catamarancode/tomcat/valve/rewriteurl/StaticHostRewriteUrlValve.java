package org.catamarancode.tomcat.valve.rewriteurl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

/**
 * This implementation will rewrite the host component of any URL that doesn't match the configured host
 * @author mkvalsvik
 *
 */
public class StaticHostRewriteUrlValve extends RewriteUrlValve {
    
    private String staticHostName;

    protected boolean processHost(URI uri, Request request, Response response) throws IOException {
        
        String host = uri.getHost().toString();
        
        if (!host.equals(this.getStaticHostName())) {
            
            URL url = uri.toURL();
            URL newUrl = new URL(url.getProtocol(), this.getStaticHostName(), url.getPort(), url.getFile());
        
            response.setContentType("text/html; charset=iso-8859-1");
            response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
            response.setHeader("Location", newUrl.toExternalForm());
            
            // Required Cache Control Headers
            String maxage = "300"; // One hour in Seconds
            response.setHeader("Cache-Control", "max-age="+ maxage);
            long relExpiresInMillis = System.currentTimeMillis() + (1000 * Long.parseLong(maxage));
            response.setHeader("Expires", getGMTTimeString(relExpiresInMillis));
 
            // Serve the file content
            OutputStream ostream = response.getOutputStream();
            String content = "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n" +
                                "<html><head><title>301 Moved Permanently</title></head><body>\n" +
                                "<h1>Moved Permanently</h1>\n" +
                                String.format("<p>The document has moved <a href=\"%s\">here</a>.</p>\n", newUrl.toExternalForm()) + 
                                "</body></html>\n";
            
            ostream.write(content.getBytes());
            ostream.flush();
            ostream.close();
            response.finishResponse();
            return true; // We have taken care of file serving.                
        }
        
        // Signal to caller that no action was taken
        return false;
    }

    public String getStaticHostName() {
        return staticHostName;
    }

    public void setStaticHostName(String staticHost) {
        this.staticHostName = staticHost;
    }
}
