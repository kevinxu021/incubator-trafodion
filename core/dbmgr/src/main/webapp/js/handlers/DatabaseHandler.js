// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var DatabaseHandler = ( function() {

			function DatabaseHandler() {
				var dispatcher = new EventDispatcher();
				var xhrs = [];
				var _this = this;
				
				this.FETCH_OBJECT_LIST_SUCCESS = 'FETCH_OBJECT_LIST_SUCCESS';
				this.FETCH_OBJECT_LIST_ERROR = 'FETCH_OBJECT_LIST_ERROR';
				this.FETCH_OBJECT_DETAILS_SUCCESS = 'FETCH_OBJECT_DETAILS_SUCCESS';
				this.FETCH_OBJECT_DETAILS_ERROR = 'FETCH_OBJECT_DETAILS_ERROR';
				this.FETCH_DDL_SUCCESS = 'FETCH_DDL_SUCCESS';
				this.FETCH_DDL_ERROR = 'FETCH_DDL_ERROR';

				
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchObjects = function(objectType, schemaName){
					var xhr = xhrs["objectlist"];
					
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/objects?type='+objectType;
					if(schemaName != null){
						uri += '&schema=' + schemaName;
					}
					
					xhrs["objectlist"] = $.ajax({
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							data.objectType = objectType;
							data.schemaName = schemaName;
							dispatcher.fire(_this.FETCH_OBJECT_LIST_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							jqXHR.objectType = objectType;
							jqXHR.schemaName = schemaName;
							dispatcher.fire(_this.FETCH_OBJECT_LIST_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchDDL = function (objectType, objectName, schemaName) {
					var xhr = xhrs["fetchddl"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/ddltext?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchddl"] = $.ajax({
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_DDL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_DDL_ERROR, jqXHR, res, error);
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

			return new DatabaseHandler();
		}());

		return DatabaseHandler;
});