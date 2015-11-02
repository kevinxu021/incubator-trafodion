// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher', 'common'],
		function(EventDispatcher, common) {"use strict";

		var SessionHandler = ( function() {

			function SessionHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;

				this.LOGIN_SUCCESS = 'loginSuccess';
				this.LOGIN_ERROR = 'loginError';
				this.LOGOUT_SUCCESS = 'logoutSuccess';
				this.LOGOUT_ERROR = 'logoutError';        	

				this.login = function(param){
					$.ajax({
						url:'resources/server/login',
						type:'POST',
						data: JSON.stringify(param),
						dataType:"json",
						contentType: "application/json;",
						async: false,
						success: function(data){
							common.storeSessionProperties(data);
							dispatcher.fire(_this.LOGIN_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.LOGIN_ERROR, jqXHR, res, error);
						}
					});
				};

				this.logout = function(param){
					$.ajax({
						url:'resources/server/logout',
						type:'POST',
						data: JSON.stringify(param),
						dataType:"json",
						contentType: "application/json;",
						success: function(data){
							dispatcher.fire(_this.LOGOUT_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.LOGOUT_ERROR, jqXHR, res, error);
						}
					});
				};                                 

				this.init = function() {
				};

				this.on = function(eventName, callback) {
					dispatcher.on(eventName, callback);
				};
				this.off = function (eventName, callback) {
					dispatcher.off(eventName, callback);
				};

				this.fire = function(eventName, eventInfo) {
					dispatcher.fire(eventName, eventInfo);
				};
			}

			return new SessionHandler();
		}());

		return SessionHandler;
});