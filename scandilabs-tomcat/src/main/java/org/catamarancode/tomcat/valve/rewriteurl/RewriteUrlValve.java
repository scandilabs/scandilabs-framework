package org.catamarancode.tomcat.valve.rewriteurl;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

public abstract class RewriteUrlValve extends ValveBase {

        protected boolean hasSubDomain(String hostName) {
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
    
        /**
         * @see org.apache.catalina.valves.ValveBase#invoke(org.apache.catalina.connector.Request,
         *      org.apache.catalina.connector.Response)
         */
        public void invoke(Request request, Response response) throws IOException,
                ServletException {
            
            String url = request.getRequestURL().toString();
            if (request.getQueryString() != null && !"".equals(request.getQueryString())) {
                url = String.format("%s?%s", url, request.getQueryString());
            }
            
            URI uri = null;
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {                
                e.printStackTrace();
                throw new RuntimeException("Invalid URI: " + url);
            }
            
            if (this.processHost(uri, request, response)) {
                
                // Terminate valve execution chain
                return;
            }
            
            // Invoke next valve or true processing
            getNext().invoke(request, response);
        }
        
        
        /**
         * Checks the host name and redirects if necessary
         * @return true if a redirect was performed, false otherwise
         */
        protected abstract boolean processHost(URI uri, Request request, Response response) throws IOException;

        
        /**
         * @see org.apache.catalina.valves.ValveBase#getInfo()
         */
        public String getInfo() {

            return getClass() + "/1.0";
        }
        
        public static String getGMTTimeString(long milliSeconds) {
            SimpleDateFormat sdf = new SimpleDateFormat("E, d MMM yyyy HH:mm:ss 'GMT'");
            return sdf.format(new Date(milliSeconds));
        }
        
    }

