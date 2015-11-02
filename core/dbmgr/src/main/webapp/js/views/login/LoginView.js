// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/login.html',
        'model/Session',
        'handlers/SessionHandler',
        'common'
        ], function (BaseView, LoginT, session, sessionHandler, common) {
	'use strict';
    var _that = null;
    var _router = null;
    
	var LoginView = Backbone.View.extend({
		
		template:  _.template(LoginT),
		
		el: $('#wrapper'),

		child: null,
		
		sessionTimedOut : false,

		initialize: function () {
			
		},
		
		init: function(){
			$('#navbar').hide();
			_that = this;
			sessionHandler.on(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.on(sessionHandler.LOGIN_ERROR, this.showErrorMessage);
			$('#loginBtn').on('click', this.loginClick);
			$('#password').on('keypress', this.passwordEnterKeyPressed);
		},
		
		resume: function(){
			sessionHandler.on(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.on(sessionHandler.LOGIN_ERROR, this.showErrorMessage);			
			$('#loginBtn').on('click', this.loginClick);
			$('#password').on('keypress', this.passwordEnterKeyPressed);
		},
		pause: function() {
			sessionHandler.off(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.off(sessionHandler.LOGIN_ERROR, this.showErrorMessage);			
			$('#loginBtn').off('click', this.loginClick);
			$('#password').off('keypress', this.passwordEnterKeyPressed);
		},
        showLoading: function(){
        	$('#loadingImg').show();
        },
        passwordEnterKeyPressed: function (ev){
        	 var keycode = (ev.keyCode ? ev.keyCode : ev.which);
	            if (keycode == '13') {
	            	_that.loginClick(ev);
	            }
        },
        hideLoading: function () {
        	$('#loadingImg').hide();
        },		
		render: function () {
			if(this.child == null){
				this.$el.html(this.template);
				this.init();
			}else
			{
				this.$el.empty().append(this.child);
				this.resume();
			}
			if(_that.sessionTimedOut == true){
				$('#login-error-text').text("Your session timed out due to inactivity. Please login again.");
				$('#login-error-text').show();
				_that.sessionTimedOut = false;
			}
			return this;        	

		},
		remove: function(){
			this.child = $(this.$el[0].firstChild).detach();
			this.pause();
		},
		loginClick: function(e){
        	
			$("#login-error-text").text("");
			$('#login-error-text').hide();
        	
			var userName = $('#username').val();
			if(userName == null || userName.length == 0) {
				alert("User Name cannot be empty");
				e.preventDefault();
				return;
			}
			var password = $('#password').val();
			if(password == null || password.length == 0) {
				alert("Password cannot be empty");
				e.preventDefault();
				return;
			}
			_that.showLoading();
			var param = {username : userName, password: password};
			sessionHandler.login(param);
        	e.preventDefault();
		},
		loginSuccess: function(result){
			$("#login-error-text").text("");
			if(result.status == 'OK'){
				session.saveToken(result.key);
                session.saveUser(result.user);
                //session.saveLoginTime(toISODateString(new Date()));
				window.location.hash = '/dashboard';
			}else{
		       	_that.hideLoading();
	        	$("#login-error-text").show();
	        	$("#login-error-text").text(result.errorMessage);
			}
		},
		doLogout: function(){
			var param = {username : session.getUser()};
			$("#login-error-text").text("");
			common.resetSessionProperties();
			session.eraseAll();
			sessionHandler.logout(param);
		},
		logoutSuccess: function(){
			
		},
        showErrorMessage: function (jqXHR, res, error) {
        	_that.hideLoading();
        	$("#login-error-text").text("");
        	$("#login-error-text").show();
        	if (jqXHR) {
        		if(jqXHR.status != null && jqXHR.status == 0) {
            		$("#login-error-text").text("Error : Unable to communicate with the server.");
        		}else {
            		$("#login-error-text").text(jqXHR.statusText);
        		}
        	}
        }  
	});


	return LoginView;
});
