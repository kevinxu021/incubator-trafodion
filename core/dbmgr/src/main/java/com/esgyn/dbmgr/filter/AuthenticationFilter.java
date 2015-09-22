// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

package com.esgyn.dbmgr.filter;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import com.esgyn.dbmgr.model.SessionModel;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AuthenticationFilter implements Filter {

  @Override
  public void destroy() {

  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse,
      FilterChain filterChain) throws IOException, ServletException {

    HttpServletRequest request = (HttpServletRequest) servletRequest;
    HttpServletResponse response = (HttpServletResponse) servletResponse;
    String currentURL = request.getRequestURI();

    String targetURL = currentURL.substring(currentURL.indexOf("/", 1), currentURL.length());
    HttpSession session = request.getSession(true);

    if (targetURL.contains("server/login")) {
      InputStream is = request.getInputStream();
      ObjectMapper mapper = new ObjectMapper();
      User user = mapper.readValue(is, User.class);
      session.setAttribute("username", user.getUsername());
      session.setAttribute("password", user.getPassword());
    } else if (targetURL.contains("server/logout")) {
      // delete this session
      session.invalidate();
    } else {
      if (session == null || session.getAttribute("username") == null) {
        String userName =
            SessionModel.getValueFromHttpServletRequest(request, SessionModel.USER_NAME);
        if (userName == null || userName.length() == 0) {
          response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
          return;
        }
      }
    }
    filterChain.doFilter(request, response);
  }

  @Override
  public void init(FilterConfig filterconfig) throws ServletException {

  }

}

/**
 * Currently just for inner use, if you wants to use it outside, refactor it as single class
 */
class User {
  private String username;
  private String password;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

}
