// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var DashboardHandler = ( function() {

			function DashboardHandler() {
				var dispatcher = new EventDispatcher();
				this.DISKREADS_SUCCESS = 'fetchDiskReadsSuccess';
				this.DISKREADS_ERROR = 'fetchDiskReadsError';        	  
				this.DISKWRITES_SUCCESS = 'fetchDiskWritesSuccess';
				this.DISKWRITES_ERROR = 'fetchDiskWritesError';        	  
				this.GETOPS_SUCCESS = 'fetchGetOpsSuccess';
				this.GETOPS_ERROR = 'fetchGetOpsError';        	  


				var _this = this;
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchCPUData = function(){
					$.ajax({
						url: 'resources/metrics/cpu',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire("fetchCPUDataSuccess", data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire("fetchCPUDataError", jqXHR, res, error);
						}
					});
				};           

				this.fetchDiskReads = function(){
					$.ajax({
						url: 'resources/metrics/diskreads',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire("fetchDiskReadsSuccess", data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire("fetchDiskReadsError", jqXHR, res, error);
						}
					});
				};

				this.fetchDiskWrites = function(){
					$.ajax({
						url: 'resources/metrics/diskwrites',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire("fetchDiskWritesSuccess", data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire("fetchDiskWritesError", jqXHR, res, error);
						}
					});
				};

				this.fetchGetOps = function(){
					$.ajax({
						url: 'resources/metrics/getops',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire("fetchGetOpsSuccess", data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire("fetchGetOpsError", jqXHR, res, error);
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

			return new DashboardHandler();
		}());

		return DashboardHandler;
});