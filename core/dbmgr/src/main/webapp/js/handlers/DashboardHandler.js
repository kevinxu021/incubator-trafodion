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
				this.TRANSACTION_STATS_SUCCESS = 'transactionsSucccess';
				this.TRANSACTION_STATS_ERROR = 'transactionsError';
				this.SUMMARY_METRIC_SUCCESS = 'SUMMARY_METRIC_SUCCESS';
				this.SUMMARY_METRIC_ERROR = 'SUMMARY_METRIC_ERROR';
				
				
				var _this = this;
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchTransactionStats = function(params){
					$.ajax({
						url: 'resources/metrics/transactions',
						type:'POSt',
						dataType:"json",
						data: JSON.stringify(params),
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

				this.fetchSummaryMetric = function(params){
					$.ajax({
						url: 'resources/metrics/summary',
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
							result.metricName = params.metricName;
							result.data = data;
							dispatcher.fire(_this.SUMMARY_METRIC_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.metricName = params.metricName;
							dispatcher.fire(_this.SUMMARY_METRIC_ERROR, jqXHR, res, error);
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