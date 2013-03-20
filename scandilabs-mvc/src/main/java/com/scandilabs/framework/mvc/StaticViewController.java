package com.scandilabs.framework.mvc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

/**
 * This controller simply maps a request to a view with the same name as the request
 * 
 * @author mkvalsvik
 * 
 */
public class StaticViewController implements Controller {

	private static Logger logger = LoggerFactory
			.getLogger(StaticViewController.class);

	public ModelAndView handleRequest(HttpServletRequest request,
			HttpServletResponse response) throws Exception {
		ModelAndView mv = new ModelAndView();
		return mv;
	}

}
