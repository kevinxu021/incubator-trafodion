define(['handlers/EventDispatcher', 'common'],
		function(EventDispatcher, common) {"use strict";

		var ServerHandler = ( function() {

			function ServerHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;

				this.FETCHDCS_SUCCESS = 'fetchDcsServersSuccess';
				this.FETCHDCS_ERROR = 'fetchDcsServersError';
				this.FETCH_SERVICES_SUCCESS = 'fetchServicesSuccess';
				this.FETCH_SERVICES_ERROR = 'fetchServicesError';
				this.FETCH_NODES_SUCCESS = 'fetchNodesSuccess';
				this.FETCH_NODES_ERROR = 'fetchNodesError';

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
							common.storeSessionProperties(data.serverTimeZone, data.serverUTCOffset, data.dcsMasterInfoUri);
						},
						error:function(jqXHR, res, error){
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