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
				this.FETCH_OBJECT_ATTRIBUTES_SUCCESS = 'FETCH_OBJECT_ATTRIBUTES_SUCCESS';
				this.FETCH_OBJECT_ATTRIBUTES_ERROR = 'FETCH_OBJECT_ATTRIBUTES_ERROR';
				this.FETCH_DDL_SUCCESS = 'FETCH_DDL_SUCCESS';
				this.FETCH_DDL_ERROR = 'FETCH_DDL_ERROR';
				this.FETCH_COLUMNS_SUCCESS = 'FETCH_COLUMNS_SUCCESS';
				this.FETCH_COLUMNS_ERROR = 'FETCH_COLUMNS_ERROR';
				this.FETCH_REGIONS_SUCCESS = 'FETCH_REGIONS_SUCCESS';
				this.FETCH_REGIONS_ERROR = 'FETCH_REGIONS_ERROR';
				this.FETCH_PRIVILEGES_SUCCESS = 'FETCH_PRIVILEGES_SUCCESS';
				this.FETCH_PRIVILEGES_ERROR = 'FETCH_PRIVILEGES_ERROR';
				this.FETCH_STATISTICS_SUCCESS = 'FETCH_STATISTICS_SUCCESS';
				this.FETCH_STATISTICS_ERROR = 'FETCH_STATISTICS_ERROR';
				this.FETCH_USAGE_SUCCESS = 'FETCH_USAGE_SUCCESS';
				this.FETCH_USAGE_ERROR = 'FETCH_USAGE_ERROR';
				this.DROP_OBJECT_SUCCESS = 'DROP_OBJECT_SUCCESS';
				this.DROP_OBJECT_ERROR = 'DROP_OBJECT_ERROR';
				
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchObjects = function(objectType, schemaName, parentObjectName){
					var xhr = xhrs["objectlist"];
					
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/objects?type='+objectType;
					if(schemaName != null){
						uri += '&schema=' + schemaName;
					}
					if(parentObjectName != null){
						uri += '&parentObjectName=' + parentObjectName;
					}
					
					xhrs["objectlist"] = $.ajax({
						cache: false,
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
							data.parentObjectName = parentObjectName;
							dispatcher.fire(_this.FETCH_OBJECT_LIST_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							jqXHR.objectType = objectType;
							jqXHR.schemaName = schemaName;
							jqXHR.parentObjectName = parentObjectName;
							dispatcher.fire(_this.FETCH_OBJECT_LIST_ERROR, jqXHR, res, error);
						}
					});
				};

				this.fetchAttributes = function(objectType, objectName, schemaName){
					var xhr = xhrs["fetchAttributes"];
					
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/attributes?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchAttributes"] = $.ajax({
						cache: false,
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
							data.objectName = objectName;
							dispatcher.fire(_this.FETCH_OBJECT_ATTRIBUTES_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							jqXHR.objectType = objectType;
							jqXHR.schemaName = schemaName;
							dispatcher.fire(_this.FETCH_OBJECT_ATTRIBUTES_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchDDL = function (objectType, objectName, schemaName, parentObjectName) {
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
					if(parentObjectName){
						uri +='&parentObjectName=' + parentObjectName;
					}
					
					xhrs["fetchddl"] = $.ajax({
						cache: false,
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
				
			
				this.fetchColumns = function (objectType, objectName, schemaName) {
					var xhr = xhrs["fetchColumns"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/columns?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchColumns"] = $.ajax({
						cache: false,
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_COLUMNS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_COLUMNS_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchRegions = function (objectType, objectName, schemaName) {
					var xhr = xhrs["fetchRegions"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/regions?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchRegions"] = $.ajax({
						cache: false,
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_REGIONS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_REGIONS_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchPrivileges = function (objectType, objectName, objectID, schemaName) {
					var xhr = xhrs["fetchPrivileges"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/privileges?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(objectID != null){
						uri += '&objectID=' + objectID;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchPrivileges"] = $.ajax({
						cache: false,
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_PRIVILEGES_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_PRIVILEGES_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchUsages = function (objectType, objectName, objectID, schemaName) {
					var xhr = xhrs["fetchUsages"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/usage?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(objectID != null){
						uri += '&objectID=' + objectID;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchUsages"] = $.ajax({
						cache: false,
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_USAGE_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_USAGE_ERROR, jqXHR, res, error);
						}
					});
				};

				this.dropObject = function (schemaName, objectType, objectName) {
					var xhr = xhrs["dropObject"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/drop';
					var params = {};
					params.objectType = objectType;
					params.objectName = objectName;
					params.schemaName = schemaName;
					
					xhrs["dropObject"] = $.ajax({
						cache: false,
						url: uri,
						type:'POST',
						dataType:"json",
						data: JSON.stringify(params),
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.schemaName = schemaName;
							result.objectName = objectName;
							dispatcher.fire(_this.DROP_OBJECT_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.schemaName = schemaName;
							jqXHR.objectName = objectName;
							dispatcher.fire(_this.DROP_OBJECT_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchStatistics = function (objectType, objectName, objectID, schemaName) {
					var xhr = xhrs["fetchStatistics"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					
					var uri = '/resources/db/statistics?type='+objectType;
					if(objectName != null){
						uri += '&objectName=' + objectName;
					}
					if(objectID != null){
						uri += '&objectID=' + objectID;
					}
					if(schemaName != null){
						uri += '&schemaName=' + schemaName;
					}
					
					xhrs["fetchStatistics"] = $.ajax({
						cache: false,
						url: uri,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_STATISTICS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_STATISTICS_ERROR, jqXHR, res, error);
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