define([
        'views/BaseView',
        'text!templates/login.html',
        'model/Session',
        'handlers/SessionHandler'
        ], function (BaseView, LoginT, session, sessionHandler) {
	'use strict';
    var _that = null;
    var _router = null;
    
	var LoginView = Backbone.View.extend({
		
		template:  _.template(LoginT),
		
		el: $('#wrapper'),

		child: null,

		initialize: function () {
			
		},
		
		init: function(router){
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
			return this;        	

		},
		remove: function(){
			this.child = $(this.$el[0].firstChild).detach();
			this.pause();
		},
		loginClick: function(e){
        	
			$("#errorText").text("");
			$('#errorText').hide();
        	
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
        	/*$.ajax({
        	    url:'resources/server/login',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
        	    success:_that.loginSuccess,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});		*/
        	e.preventDefault();
		},
		loginSuccess: function(result){
			if(result.status == 'OK'){
				session.saveToken(result.key);
                session.saveUser(result.user);
                //session.saveLoginTime(toISODateString(new Date()));
				window.location.hash = '/dashboard';
			}else{
		       	_that.hideLoading();
	        	$("#errorText").text("");
	        	$("#errorText").show();
	        	$("#errorText").text(result.errorMessage);
			}
		},
		doLogout: function(){
			var param = {username : session.getUser()};
			
			session.eraseAll();
			sessionHandler.logout(param);
			/*
        	$.ajax({
        	    url:'resources/server/logout',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
        	    success:_that.logoutSuccess,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});		*/
		},
		logoutSuccess: function(){
			
		},
        showErrorMessage: function (jqXHR, res, error) {
        	_that.hideLoading();
        	$("#errorText").text("");
        	$("#errorText").show();
        	if (jqXHR) {
        		if(jqXHR.status != null && jqXHR.status == 0) {
            		$("#errorText").text("Error : Unable to communicate with the server.");
        		}else {
            		$("#errorText").text(jqXHR.statusText);
        		}
        	}
        }  
	});


	return LoginView;
});
