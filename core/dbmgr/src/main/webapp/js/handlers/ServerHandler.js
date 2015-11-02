// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher', 'common'],
		function(EventDispatcher, common) {"use strict";

		var ServerHandler = ( function() {
			var xhrs = [];

			function ServerHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;

				this.FETCHDCS_SUCCESS = 'fetchDcsServersSuccess';
				this.FETCHDCS_ERROR = 'fetchDcsServersError';
				this.FETCH_SERVICES_SUCCESS = 'fetchServicesSuccess';
				this.FETCH_SERVICES_ERROR = 'fetchServicesError';
				this.FETCH_NODES_SUCCESS = 'fetchNodesSuccess';
				this.FETCH_NODES_ERROR = 'fetchNodesError';
				this.FETCH_ALERTS_LIST_SUCCESS = 'FETCH_ALERTS_LIST_SUCCESS';
				this.FETCH_ALERTS_LIST_ERROR = 'FETCH_ALERTS_LIST_ERROR';
				this.FETCH_VERSION_SUCCESS = 'FETCH_VERSION_SUCCESS';
				this.FETCH_VERSION_ERROR = 'FETCH_VERSION_ERROR';
				this.WRKBNCH_EXECUTE_SUCCESS = 'WRKBNCH_EXECUTE_SUCCESS';
				this.WRKBNCH_EXECUTE_ERROR = 'WRKBNCH_EXECUTE_ERROR';
				this.WRKBNCH_EXPLAIN_SUCCESS = 'WRKBNCH_EXPLAIN_SUCCESS';
				this.WRKBNCH_EXPLAIN_ERROR = 'WRKBNCH_EXPLAIN_ERROR';
								
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchDcsServers = function(){
					$.ajax({
						url: 'resources/server/dcsservers',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},    				
						success: function(data){
							dispatcher.fire(_this.FETCHDCS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCHDCS_ERROR, jqXHR, res, error);
						}
					});
				};   

				this.fetchServices = function(){
					$.ajax({
						url: 'resources/server/services',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_SERVICES_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_SERVICES_ERROR, jqXHR, res, error);
						}
					});
				};

				this.fetchNodes = function(){
					$.ajax({
						url: 'resources/server/nodes',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_NODES_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_NODES_ERROR, jqXHR, res, error);
						}
					});
				};    

				this.loadServerConfig = function(){
					$.ajax({
						url: 'resources/server/config',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						async: false,
						success: function(data){
							common.storeSessionProperties(data);
						},
						error:function(jqXHR, res, error){
						}
					});
				}; 
				
				this.explainQuery = function(param){
					$.ajax({
		        	    url:'resources/queries/explain',
		        	    type:'POST',
		        	    data: JSON.stringify(param),
		        	    dataType:"json",
		        	    contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success:  function(data){
							dispatcher.fire(_this.WRKBNCH_EXPLAIN_SUCCESS, data);
						},
		        	    error:function(jqXHR, res, error){
		        	    	dispatcher.fire(_this.WRKBNCH_EXPLAIN_ERROR, jqXHR, res, error);
		        	    }
		        	});
				};
				
				this.executeQuery = function(param){
					$.ajax({
		        	    url:'resources/queries/execute',
		        	    type:'POST',
		        	    data: JSON.stringify(param),
		        	    dataType:"json",
		        	    contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success:  function(data){
							dispatcher.fire(_this.WRKBNCH_EXECUTE_SUCCESS, data);
						},
		        	    error:function(jqXHR, res, error){
		        	    	dispatcher.fire(_this.WRKBNCH_EXECUTE_ERROR, jqXHR, res, error);
		        	    }
		        	});	
				};
				
				this.fetchAlertsList = function(params){
					var xhr = xhrs["alerts_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["alerts_list"] = $.ajax({
						url: 'resources/alerts/list',
						type:'POST',
						dataType:"json",
						data: JSON.stringify(params),						
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ALERTS_LIST_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ALERTS_LIST_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchServerInfo = function(){
					var xhr = xhrs["server_version"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["server_version"] = $.ajax({
						url: 'resources/server/about',
						type:'POST',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_VERSION_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_VERSION_ERROR, jqXHR, res, error);
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

			return new ServerHandler();
		}());

		return ServerHandler;
});