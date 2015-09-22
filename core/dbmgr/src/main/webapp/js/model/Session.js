// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([ 'jquery',
         'jquerycookie'
         ],function($) {
	"use strict";

	var Session = (function() {
		var SESSION_REFRESH = 300000; // 5 minutes
		var writeOptions = {
				path : '/',
				secure : true
		};
		var removeOptions = {
				path : '/'
		};
		var USER = "user";
		var LOGIN_TIME = "loginTime";
		var TOKEN = "token";
		var HISTORY_LOCATION = "hisLocation";
		
		function Session() {
			var timer = null;
			
			function getCookie(s) {
				return $.cookie(s);
			}

			function setCookie(s, val) {
				writeOptions.secure = (window.location.protocol === "https:");
				$.cookie(s, val, writeOptions);
			}

			function delCookie(s) {
				$.removeCookie(s, removeOptions);
			}
			
            function reset() {
                //timer = setInterval(accessSession, SESSION_REFRESH);
            }

            function stopTimer() {
                timer = clearInterval(timer);
            }
			
			this.getUser = function() {
				return getCookie(USER);
			};

			this.saveUser = function(user) {
				setCookie(USER, user);
			};

			this.eraseUser = function() {
				delCookie(USER);
			};    
			
			this.saveToken = function(auth) {
                setCookie(TOKEN, auth);
                reset();
            };

            this.getToken = function() {
                return getCookie(TOKEN);
            };

            this.eraseToken = function() {
                delCookie(TOKEN);
                stopTimer();
            };
            
            this.saveLoginTime = function(loginTime) {
                setCookie(LOGIN_TIME, loginTime);
            };

            this.getLoginTime = function() {
                return getCookie(LOGIN_TIME);
            };

            this.eraseLoginTime = function() {
                delCookie(LOGIN_TIME);
            };
            
            this.saveHistoryLocation = function(hl) {
                setCookie(HISTORY_LOCATION, hl);
            };

            
            this.getHistoryLocation = function() {
                return getCookie(HISTORY_LOCATION);
            };

            this.eraseHistoryLocation = function() {
                delCookie(HISTORY_LOCATION);
            };
            
            this.eraseAll = function() {
                this.eraseToken();
                this.eraseUser();
                this.eraseLoginTime();
                //this.eraseHistoryLocation();
            };
		}

		return new Session();
	}());
	return Session;
});
