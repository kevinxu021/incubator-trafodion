//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/login.html',
        'model/Session',
        'handlers/SessionHandler',
        'common',
        'jqueryvalidate'
     ], function (BaseView, LoginT, session, sessionHandler, common) {
	'use strict';
	var _that = null;
	var _router = null;
	var validator = null;
	var LOGIN_FORM = '#login-form',
		ALERTS_FEATURE = '#alerts-feature',
		ERROR_TEXT = '#login-error-text',
		SPINNER = '#loadingImg',
		LOGIN_BUTTON = '#loginBtn',
		USERNAME = '#username',
		PASSWORD = '#password';
	
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
			
			validator = $(LOGIN_FORM).validate({
				rules: {
					"username": { required: true },
					"password": { required: true}
				},
				messages: {
					username: "Please enter your user name",
		                    	password: "Please provide a password"
		                },
				highlight: function(element) {
					$(element).closest('.form-group').addClass('has-error');
				},
				unhighlight: function(element) {
					$(element).closest('.form-group').removeClass('has-error');
				},
				errorElement: 'span',
				errorClass: 'help-block',
				errorPlacement: function(error, element) {
					if(element.parent('.input-group').length) {
						error.insertAfter(element.parent());
					} else {
						error.insertAfter(element);
					}
				}
			});
			
			sessionHandler.on(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.on(sessionHandler.LOGIN_ERROR, this.showErrorMessage);
			$(LOGIN_BUTTON).on('click', this.loginClick);
			$(PASSWORD).on('keypress', this.passwordEnterKeyPressed);
		},

		resume: function(){
			sessionHandler.on(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.on(sessionHandler.LOGIN_ERROR, this.showErrorMessage);			
			$(LOGIN_BUTTON).on('click', this.loginClick);
			$(PASSWORD).on('keypress', this.passwordEnterKeyPressed);
		},
		pause: function() {
			sessionHandler.off(sessionHandler.LOGIN_SUCCESS, this.loginSuccess);
			sessionHandler.off(sessionHandler.LOGIN_ERROR, this.showErrorMessage);			
			$(LOGIN_BUTTON).off('click', this.loginClick);
			$(PASSWORD).off('keypress', this.passwordEnterKeyPressed);
		},
		showLoading: function(){
			$(SPINNER).show();
		},
		passwordEnterKeyPressed: function (ev){
			var keycode = (ev.keyCode ? ev.keyCode : ev.which);
			if (keycode == '13') {
				_that.loginClick(ev);
			}
		},
		hideLoading: function () {
			$(SPINNER).hide();
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
				$(ERROR_TEXT).text("Your session timed out due to inactivity. Please login again.");
				$(ERROR_TEXT).show();
				_that.sessionTimedOut = false;
			}
			return this;        	

		},
		remove: function(){
			this.child = $(this.$el[0].firstChild).detach();
			this.pause();
		},
		loginClick: function(e){

			$(ERROR_TEXT).text("");
			$(ERROR_TEXT).hide();

			if(!$(LOGIN_FORM).valid()){
				e.preventDefault();
				return;
			}
			
			var userName = $(USERNAME).val();
			var password = $(PASSWORD).val();
			_that.showLoading();
			var param = {username : userName, password: password};
			sessionHandler.login(param);
			e.preventDefault();
		},
		loginSuccess: function(result){
			$(ERROR_TEXT).text("");
			if(result.status == 'OK'){
				session.saveToken(result.key);
				session.saveUser(result.user);
				if(result.enableAlerts != null && result.enableAlerts == false){
					$(ALERTS_FEATURE).hide();
				}else{
					$(ALERTS_FEATURE).show();
				}
				//session.saveLoginTime(toISODateString(new Date()));
				window.location.hash = '/dashboard';
			}else{
				_that.hideLoading();
				$(ERROR_TEXT).show();
				$(ERROR_TEXT).text(result.errorMessage);
			}
		},
		doLogout: function(){
			var param = {username : session.getUser()};
			$(ERROR_TEXT).text("");
			common.resetSessionProperties();
			session.eraseAll();
			sessionHandler.logout(param);
		},
		logoutSuccess: function(){

		},
		showErrorMessage: function (jqXHR, res, error) {
			_that.hideLoading();
			$(ERROR_TEXT).text("");
			$(ERROR_TEXT).show();
			if (jqXHR) {
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(ERROR_TEXT).text("Error : Unable to communicate with the server.");
				}else {
					$(ERROR_TEXT).text(jqXHR.statusText);
				}
			}
		}  
	});


	return LoginView;
});
