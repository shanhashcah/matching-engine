package com.exchange.controller;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class SimpleCORSFilter implements Filter {

private final Logger log = LoggerFactory.getLogger(SimpleCORSFilter.class);

static final String ORIGIN = "Origin";

public SimpleCORSFilter() {
	
    log.info("SimpleCORSFilter init");
}

@Override
public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {

    HttpServletRequest request = (HttpServletRequest) req;
    HttpServletResponse response = (HttpServletResponse) res;
   // System.out.println(request.getHeader(ORIGIN));
    //System.out.println(request.getMethod());

    response.setHeader("Access-Control-Allow-Origin",request.getHeader(ORIGIN));
    response.setHeader("Access-Control-Allow-Credentials", "true");
    response.setHeader("Access-Control-Allow-Methods", "POST, GET, OPTIONS");
    response.addHeader("Access-Control-Allow-Headers", "Authorization, X-PINGOTHER, Origin, X-Requested-With, Content-Type, Accept");
    response.setHeader("Access-Control-Max-Age", "3600");
    //response.setHeader("Access-Control-Allow-Headers", "Authorization,Content-Type, Accept, X-Requested-With, remember-me");
    //response.setHeader("Access-Control-Allow-Headers",request.getHeader("Access-Control-Request-Headers"));
    chain.doFilter(request, response);
}

@Override
public void init(FilterConfig filterConfig) {
}

@Override
public void destroy() {
}

}