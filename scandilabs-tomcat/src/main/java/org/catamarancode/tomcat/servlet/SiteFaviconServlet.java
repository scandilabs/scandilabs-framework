package org.catamarancode.tomcat.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.servlets.DefaultServlet;

public class SiteFaviconServlet extends DefaultServlet {
	
    private static final long serialVersionUID = 1L;
    
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        response.setContentType("image/x-icon");
        super.doGet(request, response);
    }
}