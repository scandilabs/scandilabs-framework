package org.catamarancode.spring.mvc;

import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.HandlerExceptionResolver;
import org.springframework.web.servlet.ModelAndView;

/**
 * An exception resolver that displays an html page with stack trace with
 * a unique reference code or a Json error.
 */
public class SimpleHtmlExceptionResolver implements HandlerExceptionResolver {

	private Logger logger = LoggerFactory
			.getLogger(SimpleHtmlExceptionResolver.class);

	private String errorView = "error";
	private String live;
	public String getErrorView() {
		return errorView;
	}

	public String getLive() {
		return live;
	}
	public boolean isProduction() {
		return BooleanUtils.toBoolean(live, "true", "false");
	}

	public ModelAndView resolveException(HttpServletRequest request,
			HttpServletResponse response, Object handler, Exception ex) {
		
		// Generate unique error code
		String reference = RandomStringUtils.randomAlphanumeric(10)
				.toUpperCase();

		ModelAndView mv = null;
		
		// This is a regular browser http requst. So we render error as html
		mv = new ModelAndView(errorView);
		
		// Useful error statistics
		mv.addObject("logExceptionTime", new Date());
		mv.addObject("logExceptionID", reference);
		
		// Include error code if known
		String errorMessage = ex.getMessage();
		mv.addObject("logExceptionMessage", errorMessage);		

		// Only show stack trace if we're not in production mode
		if (!isProduction()) {
				String stack = ExceptionUtils.getFullStackTrace(ex).replaceAll(
						"\n", "<br>");
				mv.addObject("logException", stack);
		}
		
		logger.error(String.format("Handled error via HTML reference %s: %s", reference, errorMessage), ex);

		return mv;
	}

	public void setErrorView(String errorView) {
		this.errorView = errorView;
	}

	public void setLive(String live) {
		this.live = live;
	}

	
}
