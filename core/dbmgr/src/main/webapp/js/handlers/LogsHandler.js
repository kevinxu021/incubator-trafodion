// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var LogsHandler = ( function() {
			var xhrs = [];

			function LogsHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;

				this.FETCHLOGS_SUCCESS = 'fetchLogsSuccess';
				this.FETCHLOGS_ERROR = 'fetchLogsError';

				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchLogs = function(params){
					var xhr = xhrs["logs_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["logs_list"] = $.ajax({
						url: 'resources/logs/list',
						type:'POST',
						data: JSON.stringify(params),
						dataType:"json",
						contentType: "application/json",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(result){
							dispatcher.fire(_this.FETCHLOGS_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCHLOGS_ERROR, jqXHR, res, error);
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

			return new LogsHandler();
		}());

		return LogsHandler;
});