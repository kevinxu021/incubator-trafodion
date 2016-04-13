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

				this.FETCHDCS_SUCCESS = 'FETCHDCS_SUCCESS';
				this.FETCHDCS_ERROR = 'FETCHDCS_ERROR';
				this.DCS_SUMMARY_SUCCESS = 'DCS_SUMMARY_SUCCESS';
				this.DCS_SUMMARY_ERROR = 'DCS_SUMMARY_ERROR';
				
				this.PSTACK_SUCCESS = 'PSTACK_SUCCESS';
				this.PSTACK_ERROR = 'PSTACK_ERROR';
				this.FETCH_SERVICES_SUCCESS = 'fetchServicesSuccess';
				this.FETCH_SERVICES_ERROR = 'fetchServicesError';
				this.FETCH_NODES_SUCCESS = 'fetchNodesSuccess';
				this.FETCH_NODES_ERROR = 'fetchNodesError';
				
				this.FETCH_ALERTS_LIST_SUCCESS = 'FETCH_ALERTS_LIST_SUCCESS';
				this.FETCH_ALERTS_LIST_ERROR = 'FETCH_ALERTS_LIST_ERROR';
				this.FETCH_ALERT_DETAIL_SUCCESS = 'FETCH_ALERT_DETAIL_SUCCESS';
				this.FETCH_ALERT_DETAIL_ERROR = 'FETCH_ALERT_DETAIL_ERROR';
				this.ALERT_UPDATE_SUCCESS = 'ALERT_UPDATE_SUCCESS';
				this.ALERT_UPDATE_ERROR = 'ALERT_UPDATE_ERROR';
				
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
					var xhr = xhrs["fetchDcsServers"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["fetchDcsServers"] = $.ajax({
						cache: false,
						url: 'resources/server/dcs/servers',
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
				
				this.fetchDcsSummary = function(){
					var xhr = xhrs["fetchDcsSummary"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["fetchDcsSummary"] = $.ajax({
						cache: false,
						url: 'resources/server/dcs/summary',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},    				
						success: function(data){
							dispatcher.fire(_this.DCS_SUMMARY_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.DCS_SUMMARY_ERROR, jqXHR, res, error);
						}
					});
				}; 

				this.getPStack = function(processID, processName){
					var xhr = xhrs["getPStack"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["getPStack"] = $.ajax({
						cache: false,
						url: 'resources/server/pstack/'+processID,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},    				
						success: function(data){
							var result = {};
							result.processID = processID;
							result.processName = processName;
							result.pStack = JSON.stringify(data[0].PROGRAM);
							dispatcher.fire(_this.PSTACK_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							var result = {};
							result.processID = processID;
							result.processName = processName;
							result.pStack = jqXHR.responseText;
							dispatcher.fire(_this.PSTACK_ERROR, result);
						}
					});
				}; 
				
				this.fetchServices = function(){
					var xhr = xhrs["fetchServices"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["fetchServices"] = $.ajax({
						cache: false,
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
					var xhr = xhrs["fetchNodes"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					xhrs["fetchNodes"] = $.ajax({
						cache: false,
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
					var xhr = xhrs["server_config"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["server_config"] = $.ajax({
						cache: false,
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
					var xhr = xhrs["explain_query"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["explain_query"] = $.ajax({
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
					var xhr = xhrs["execute_query"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["execute_query"] = $.ajax({
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
				
				this.fetchAlertDetail = function(params){
					var xhr = xhrs["alert_detail"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["alert_detail"] = $.ajax({
						url: 'resources/alerts/detail',
						type:'POST',
						dataType:"json",
						data: JSON.stringify(params),						
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ALERT_DETAIL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ALERT_DETAIL_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.updateAlert = function(params){
					var xhr = xhrs["update_alert"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["update_alert"] = $.ajax({
						url: 'resources/alerts/action',
						type:'POST',
						dataType:"json",
						data: JSON.stringify(params),						
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.ALERT_UPDATE_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.ALERT_UPDATE_ERROR, jqXHR, res, error);
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