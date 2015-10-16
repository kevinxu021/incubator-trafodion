// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define(['handlers/EventDispatcher'],
		function(EventDispatcher) {"use strict";

		var DashboardHandler = ( function() {
			var xhrs = [];
			
			function DashboardHandler() {
				var dispatcher = new EventDispatcher();
				this.TRANSACTION_STATS_SUCCESS = 'transactionsSucccess';
				this.TRANSACTION_STATS_ERROR = 'transactionsError';
				this.SUMMARY_METRIC_SUCCESS = 'SUMMARY_METRIC_SUCCESS';
				this.SUMMARY_METRIC_ERROR = 'SUMMARY_METRIC_ERROR';
				this.DRILLDOWN_METRIC_ERROR = 'DRILLDOWN_METRIC_ERROR';
				this.DRILLDOWN_METRIC_SUCCESS = 'DRILLDOWN_METRIC_SUCCESS';
				
				var _this = this;
				this.sessionTimeout = function() {
					window.location.hash = '/stimeout';
				};

				this.fetchSummaryMetric = function(params){
					var xhr = xhrs[params.metricName];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs[params.metricName] = $.ajax({
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
							result.timeinterval = params.timeinterval;
							result.data = data;
							dispatcher.fire(_this.SUMMARY_METRIC_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.metricName = params.metricName;
							dispatcher.fire(_this.SUMMARY_METRIC_ERROR, jqXHR, res, error);
						}
					});
				};
				
				this.fetchMetricDrilldown = function(params){
					var xhr = xhrs[params.metricName];
					if(xhr && xhr.readyState !=4){
						xhr.abort();
					}
					xhrs[params.metricName] = $.ajax({
						url: 'resources/metrics/drilldown',
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
							result.timeinterval = params.timeinterval;
							result.data = data;
							dispatcher.fire(_this.DRILLDOWN_METRIC_SUCCESS, result);
						},
						error:function(jqXHR, res, error){
							jqXHR.metricName = params.metricName;
							dispatcher.fire(_this.DRILLDOWN_METRIC_ERROR, jqXHR, res, error);
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