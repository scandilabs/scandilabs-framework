package org.catamarancode.tomcat.valve.rewriteurl;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class URIUtils {
    
    /**
     * Tester method
     * @param args
     */
    public static void main(String[] args) {
        try {
            URI uri = new URI("http", null, "www.postpo.st", -1, "/search", "q=silverton",
                    "mysilvertonpostpost");
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
    
    private URIUtils() {}
    
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
    
    /**
     * Uses a fairly simple notion of an IP address...doesn't take ports into account
     * for example, nor does it attempt to match more precisely according to valid IP
     * groups.
     * @param hostName
     * @return
     */
    public static boolean isIPAddress(String hostName) {
    	String ipPattern = "(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})";
        Pattern pattern = Pattern.compile(ipPattern);
        Matcher matcher = pattern.matcher(hostName);
        return matcher.find();
    }
    
    public static String stripSubdomain(String hostName) {
        if (hasSubDomain(hostName)) {                
                int firstDotPos = hostName.indexOf(".");
                return hostName.substring(firstDotPos+1);
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
            returnURI = new URI(uri.getScheme(), null, stripSubdomain(hostName), port, uri.getPath(), uri.getQuery(),
                    uri.getFragment());
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Problems with uri %s", uri.toString()), e);
        }
        return returnURI;
    }

}
