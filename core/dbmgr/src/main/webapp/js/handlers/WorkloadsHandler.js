// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var WorkloadsHandler = ( function() {
			var xhrs = [];

			function WorkloadsHandler() {
				var dispatcher = new EventDispatcher();
				var _this = this;
				this.FETCH_REPO_SUCCESS = 'fetchRespoSuccess';
				this.FETCH_REPO_ERROR = 'fetchRespoError';
				this.FETCH_ACTIVE_SUCCESS = 'fetchActiveSuccess';
				this.FETCH_ACTIVE_ERROR = 'fetchActiveError';
				this.FETCH_REPO_QUERY_DETAIL_SUCCESS = 'fetchRepoQDetailSuccess';
				this.FETCH_REPO_QUERY_DETAIL_ERROR = 'fetchRepoQDetailError';
				this.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS = 'fetchActiveQDetailSuccess';
				this.FETCH_ACTIVE_QUERY_DETAIL_ERROR = 'fetchActiveQDetailError';
				this.ACTIVE_CANCEL_QUERY_SUCCESS = 'activeCancelQuerySuccess';
				this.ACTIVE_CANCEL_QUERY_ERROR = 'activeCancelQueryError';
				this.HISTORICAL_CANCEL_QUERY_SUCCESS = 'historicalCancelQuerySuccess';
				this.HISTORICAL_CANCEL_QUERY_ERROR = 'historicalCancelQueryError';
				this.PLAN_CANCEL_QUERY_SUCCESS = 'planCancelQuerySuccess';
				this.PLAN_CANCEL_QUERY_ERROR = 'planCancelQueryError';

				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchQueriesInRepository = function(params){
					var xhr = xhrs["repo_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["repo_list"] = $.ajax({
						url: 'resources/workloads/repo',
						type:'POST',
						data: JSON.stringify(params),
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_REPO_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_REPO_ERROR, jqXHR, res, error);
						}
					});
				}; 

				this.fetchRepositoryQueryDetail = function(queryID){

					var xhr = xhrs["repo_detail"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["repo_detail"] = $.ajax({
						cache: false,
						url: 'resources/workloads/repo/detail?queryID=' + queryID,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_REPO_QUERY_DETAIL_ERROR, jqXHR, res, error);
						}
					});
				};

				this.cancelQuery = function(queryID,pageIdentifier){

					var xhr = xhrs["cancel_query"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["cancel_query"] = $.ajax({
						cache: false,
						url: 'resources/workloads/cancel?queryID=' + queryID,
						type:'DELETE',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							var result = {};
							result.data = data;
							result.queryID = queryID;
							switch (pageIdentifier) {
								case "active":
									dispatcher.fire(_this.ACTIVE_CANCEL_QUERY_SUCCESS, result);
									break;
								case "historical":
									dispatcher.fire(_this.HISTORICAL_CANCEL_QUERY_SUCCESS, result);
									break;
								case "queryPlan":
									dispatcher.fire(_this.PLAN_CANCEL_QUERY_SUCCESS, result);
									break;
								}
							
						},
						error:function(jqXHR, res, error){
							jqXHR.queryID=queryID;
							switch (pageIdentifier) {
							case "active":
								dispatcher.fire(_this.ACTIVE_CANCEL_QUERY_ERROR, jqXHR, res, error);
								break;
							case "historical":
								dispatcher.fire(_this.HISTORICAL_CANCEL_QUERY_ERROR, jqXHR, res, error);
								break;
							case "queryPlan":
								dispatcher.fire(_this.PLAN_CANCEL_QUERY_ERROR, jqXHR, res, error);
								break;
							}
						}
					});
				};            
				this.fetchActiveQueries = function(){

					var xhr = xhrs["active_list"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["active_list"] = $.ajax({
						cache: false,
						url: 'resources/workloads/active',
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ACTIVE_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ACTIVE_ERROR, jqXHR, res, error);
						}
					});
				};

				this.fetchActiveQueryDetail = function(queryID){

					var xhr = xhrs["active_detail"];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs["active_detail"] = $.ajax({
						cache: false,
						url: 'resources/workloads/active/detailnew?queryID=' + queryID,
						type:'GET',
						dataType:"json",
						contentType: "application/json;",
						statusCode : {
							401 : _this.sessionTimeout,
							403 : _this.sessionTimeout
						},
						success: function(data){
							dispatcher.fire(_this.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, data);
						},
						error:function(jqXHR, res, error){
							dispatcher.fire(_this.FETCH_ACTIVE_QUERY_DETAIL_ERROR, jqXHR, res, error);
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

			return new WorkloadsHandler();
		}());

		return WorkloadsHandler;
});