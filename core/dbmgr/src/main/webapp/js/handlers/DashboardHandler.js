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
				this.CANARY_SUCCESS = 'canarySuccess';
				this.CANARY_FAILURE = 'canaryFailure';
				this.TRANSACTION_STATS_SUCCESS = 'transactionsSucccess';
				this.TRANSACTION_STATS_ERROR = 'transactionsError';
				this.DISKSPACEUSED_SUCCESS = 'diskSpaceUsedSuccess';
				this.DISKSPACEUSED_ERROR = 'diskSpaceUsedError';
				this.IOWAIT_SUCCESS = 'iowaitSuccess';
				this.IOWAIT_ERROR = 'iowaitError';
				this.GCTIME_SUCCESS = 'gctimeSuccess';
				this.GCTIME_ERROR = 'gctimeError';
				this.RSRVR_MEMORY_SUCCESS = 'rsrvrMemorySuccess';
				this.RSRVR_MEMORY_ERROR = 'rsrvrMemoryError';
				this.MEMSTORE_SUCCESS = 'memstoreSuccess';
				this.MEMSTORE_ERROR = 'memStoreError';
				
				
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
				
				this.fetchCanaryResponse = function(){
					$.ajax({
						url: 'resources/metrics/canary',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.CANARY_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.CANARY_ERROR, jqXHR, res, error);
						}
					});
				};           


				this.fetchTransactionStats = function(){
					$.ajax({
						url: 'resources/metrics/transactions',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.TRANSACTION_STATS_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.TRANSACTION_STATS_ERROR, jqXHR, res, error);
						}
					});
				};           

				this.fetchUsedDiskSpace = function(){
					$.ajax({
						url: 'resources/metrics/useddiskspace',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.DISKSPACEUSED_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.DISKSPACEUSED_ERROR, jqXHR, res, error);
						}
					});
				};   
				
				this.fetchIOWaits = function(){
					$.ajax({
						url: 'resources/metrics/iowaits',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.IOWAIT_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.IOWAIT_ERROR, jqXHR, res, error);
						}
					});
				};  
				
				this.fetchGCTime = function(){
					$.ajax({
						url: 'resources/metrics/jvmgctime',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.GCTIME_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.GCTIME_ERROR, jqXHR, res, error);
						}
					});
				};  
				
				this.fetchRegionServerMemoryUsage = function(){
					$.ajax({
						url: 'resources/metrics/regionservermemory',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.RSRVR_MEMORY_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.RSRVR_MEMORY_ERROR, jqXHR, res, error);
						}
					});
				};  
				
				this.fetchMemStoreSize = function(){
					$.ajax({
						url: 'resources/metrics/memstoresize',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.MEMSTORE_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.MEMSTORE_ERROR, jqXHR, res, error);
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