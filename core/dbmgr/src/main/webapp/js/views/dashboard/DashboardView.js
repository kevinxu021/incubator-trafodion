// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
    'views/BaseView',
    'raphael',
    'morris',
    'text!templates/dashboard.html',
    'handlers/DashboardHandler',
    'handlers/ServerHandler',
    'jquery',
    'common',
    'moment',
    'views/RefreshTimerView',
    'views/TimeRangeView',
    'datatables',
    'datatablesBootStrap',
    'tabletools',
    'responsivetable'
], function (BaseView, Raphael, Morris, DashboardT, dashboardHandler, serverHandler, $, common, moment, refreshTimer, timeRangeView) {
    'use strict';

    var _this = null;
    var SERVICES_SPINNER = '#services-spinner',
	    SERVICES_RESULT_CONTAINER = '#services-result-container',
	    SERVICES_ERROR_TEXT = '#services-error-text',
	    
	    NODES_SPINNER = '#nodes-spinner',
	    NODES_RESULT_CONTAINER = '#nodes-result-container',
	    NODES_ERROR_TEXT = '#nodes-error-text',

	    TRANSACTIONS_SPINNER = '#transactions-spinner',
	    TRANSACTIONS_RESULT_CONTAINER = '#transactions-chart',
	    TRANSACTIONS_ERROR_TEXT = '#transactions-error-text',

	    REFRESH_ACTION = '#refreshAction',
	    OPEN_FILTER = '#openFilter';
    
    var transactionsGraph = null;
    
    var renderedCharts = {};
    
    var chartConfig = null;
    
    var servicesTable = null, nodesTable = null;
    
    var DashboardView = BaseView.extend({
    	template:  _.template(DashboardT),
    	
		doInit: function (){
			
			_this = this;
			refreshTimer.init();
			timeRangeView.init();
			chartConfig =  {
        		canary:{
        			xtimemultiplier: 1000,
        			ylabel: "Canary Response Time",
        			ylabelunit: "msec",
        			yvalformatter: common.formatNumberWithComma,
        			spinner:"#canary-spinner", 
        			graphcontainer:"canary-chart", 
        			errorcontainer:"#canary-error-text",
        			},
        		iowaits:{
        			xtimemultiplier: 1000,
        			ylabel: "IO Waits",
        			ylabelunit: "",
        			spinner:"#iowaits-spinner",
        			graphcontainer:"iowaits-chart",
        			errorcontainer:"#iowaits-error-text"},
        		useddiskspace:{
        			xtimemultiplier: 1,
        			ylabel: "Disk Space Used",
        			ylabelunit: "%",
        			spinner:"#useddiskspace-spinner",
        			graphcontainer:"useddiskspace-chart",
        			errorcontainer:"#useddiskspace-error-text"},
        		memstoresize:{
        			xtimemultiplier: 1,
        			ylabel: "Memstore Size",
        			ylabelunit: "MB",
        			yLabelFormat: common.convertToMB,
        			yvalformatter: common.convertToMB,
        			spinner:"#memstoresize-spinner",
        			graphcontainer:"memstoresize-chart",
        			errorcontainer:"#memstoresize-error-text"},
        		jvmgctime:{
        			xtimemultiplier: 1,
        			ylabel: "GC Time",
        			ylabelunit: "msec",
        			spinner:"#jvmgctime-spinner",
        			graphcontainer:"jvmgctime-chart",
        			errorcontainer:"#jvmgctime-error-text"},
        		regionservermemory:{
        			xtimemultiplier: 1,
        			ylabel: "Avg. Memory Usage",
        			ylabelunit: "MB",
        			spinner:"#regionservermemory-spinner",
        			graphcontainer:"regionservermemory-chart",
        			errorcontainer:"#regionservermemory-error-text"}
			};
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$(NODES_ERROR_TEXT).hide();

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);

			dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
			dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);
			
			
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.on(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);
			timeRangeView.eventAgg.on(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);
			refreshTimer.setRefreshInterval(0.5);
			timeRangeView.setTimeRange(1);
			this.refreshPage();
		},
		doResume: function(){
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$('#nodes-error-text').hide();

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);
			
			dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
			dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);

			$(REFRESH_ACTION).on('click', this.refreshPage);
			
			refreshTimer.resume();
			timeRangeView.resume();
			
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.on(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);	
			timeRangeView.eventAgg.on(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);
			
			this.refreshPage();
		},
		doPause: function(){
			refreshTimer.pause();
			timeRangeView.pause();
			
			serverHandler.off(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.off(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.off(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.off(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.off(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.off(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);

			dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
			dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);

			$(REFRESH_ACTION).off('click', this.refreshPage);
			
			refreshTimer.eventAgg.off(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.off(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);	
			timeRangeView.eventAgg.off(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);
		},
        showLoading: function(){
        	$('#cpuLoadingImg').show();
        },

        hideLoading: function () {
        	$('#cpuLoadingImg').hide();
        },
        timeRangeChanged: function(){
        	refreshTimer.restart();
        	_this.refreshPage();
        },
        refreshIntervalChanged: function(){

        },
        timerBeeped: function(){
        	_this.refreshPage();
        },
        generateParams: function(metricName){
        	var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
        	var endTime = $('#enddatetimepicker').data("DateTimePicker").date();

        	var params = {};
        	params.startTime = startTime.format('YYYY/MM/DD-HH:mm:ss');
        	params.endTime = endTime.format('YYYY/MM/DD-HH:mm:ss');
        	params.metricName = metricName;
        	return params;
        },
        refreshPage: function() {
        	timeRangeView.updateFilter();
        	_this.fetchServices();
        	_this.fetchNodes();
        	_this.fetchTransactionStats(_this.generateParams('transactions'));

        	$.each(Object.getOwnPropertyNames(chartConfig), function(k, v){
        		dashboardHandler.fetchSummaryMetric(_this.generateParams(v));
        	});
        },
        fetchServices: function () {
			$(SERVICES_SPINNER).show();
			serverHandler.fetchServices();       	
        },
        fetchServicesSuccess: function(result) {
        	$(SERVICES_ERROR_TEXT).hide();
        	$(SERVICES_RESULT_CONTAINER).show();
        	$(SERVICES_SPINNER).hide();
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table dt-responsive" id="services-results"></table>';
				$(SERVICES_RESULT_CONTAINER).html( sb );

				var aoColumns = [];
				
				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});
				
				var obj = new Object();
				obj.title = "STATUS";
				aoColumns.push(obj);
				
				var aaData = [];
				$.each(result.resultArray, function(i, data){
					var status = 'OK';
					var configured = parseInt(data[1]);
					var actuals = parseInt(data[2]);
					if(configured == 0 || actuals == 0){
						status = 'ERROR';
					}else{
						if(configured != actuals)
							status = 'Warning';
						var percentUp = (actuals/configured)*100; 
						if( percentUp < 30){
							status = 'ERROR';
						}
					}
					
					data.push(status);
					aaData.push(data);
				});

				servicesTable = $('#services-results').dataTable({
					dom: '<"clear">rt',
					"bProcessing": true,
					"bPaginate" : false, 
					"bAutoWidth": false,
					"scrollCollapse": true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ 
					    { "aTargets": [ 0 ], sTitle: "SERVICE", "mData": 0, "sWidth":"40px",
					    	"mRender": function ( data, type, full ) {
							       if (type === 'display') {
							    	   if(data == 'DTM'){
							    		   return 'Transaction Manager';
							    	   }
							    	   if(data == 'RMS'){
							    		   return 'Runtime Management Service';
							    	   }
							    	   if(data == 'MXOSRVR'){
							    		   return 'Connectivity Service';
							    	   }
						    		   return data;
							        }
							        else return data;
							      }
					    },
					    {"aTargets": [ 1 ], "sClass":"never", "bVisible":false},
					    {"aTargets": [ 2 ], "sClass":"never", "bVisible":false},
					    {"aTargets": [ 3 ], "sClass":"never", "bVisible":false},
					    {
						      "aTargets": [ 4 ],
						      "mData": 4,
						      "sWidth":"50px",
						      "mRender": function ( data, type, full ) {
						       if (type === 'display') {
						    	   if(data == 'ERROR'){
						    		   return '<button type="button" class="btn btn-danger btn-circle btn-small" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
						    		   'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0")+'"><i class="fa fa-times"></i></button>';
						    	   }
						    	   if(data == 'WARN'){
						    		   return '<button type="button" class="btn btn-warning btn-circle btn-small" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
						    		   'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0")+'"><i class="fa fa-warning"></i></button>';
						    	   }
					    		   return '<button type="button" class="btn btn-success btn-circle btn-small" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
					    		   'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0") +'"><i class="fa fa-check"></i></button>';
						        }
						        else return data;
						      }
						    }					    
					    ],
					paging: true,
					/*"tableTools": {
						"sRowSelect": "none",
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},*/
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             },
		             initComplete: function ( settings, json ) {
		     			//activate the bootstrap toggle js
		     			//must be done within initcomplete (ie after table data is loaded)
		     			$('[data-toggle="tooltip"]').tooltip({
		     				trigger: 'hover',
		     				html: true
		     				});
		     					
		     			}// end of initcomplete*/
				});
				
				
				$('#services-results td').css("white-space","nowrap");
			}
        	
        },
        fetchServicesError: function(jqXHR, res, error) {
        	$(SERVICES_SPINNER).hide();
        	$(SERVICES_RESULT_CONTAINER).hide();
        	$(SERVICES_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
        		$(SERVICES_ERROR_TEXT).text(jqXHR.responseText);     
        	}       	
        },
        fetchNodes: function () {
			$(NODES_SPINNER).show();
			serverHandler.fetchNodes();       	
        },
        fetchNodesSuccess: function(result) {
        	$(NODES_SPINNER).hide();
        	$(NODES_RESULT_CONTAINER).show();
        	$(NODES_ERROR_TEXT).hide();  
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table dt-responsive" id="nodes-results"></table>';
				$(NODES_RESULT_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});
				$.each(result.resultArray, function(i, data){
					aaData.push(data);
				});

				var bPaging = aaData.length > 10;

				nodesTable = $('#nodes-results').dataTable({
					dom: 'T<"clear">lfrtip',
					"bProcessing": true,
					"bPaginate" : bPaging, 
					"iDisplayLength" : 10, 
					"sPaginationType": "simple_numbers",
					"scrollCollapse": true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [
					      {"aTargets": [0], "sWidth": "100px"},
					      {
					      "aTargets": [ 1 ],
					      "mData": 1,
					      "sWidth":"50px",
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					    	   if(data == 'DOWN'){
					    		   return '<button type="button" class="btn btn-danger btn-circle btn-small"><i class="fa fa-times"></i></button>';
					    	   }
				    		   return '<button type="button" class="btn btn-success btn-circle btn-small"><i class="fa fa-check"></i></button>';
					        }
					        else return data;
					      }
					    }],
					paging: true,
					"tableTools": {
						"sRowSelect": "multi",
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             }
				});
				$('#nodes-results td').css("white-space","nowrap");
			}
        	        	
        },
        fetchNodesError: function(jqXHR, res, error) {
        	$(NODES_SPINNER).hide();
        	$(NODES_RESULT_CONTAINER).hide();
        	$(NODES_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
        		if(jqXHR.responseText.indexOf("No node resources found") > 0){
        			$(NODES_ERROR_TEXT).text("Node information is not available.");
        		}else
        			$(NODES_ERROR_TEXT).text(jqXHR.responseText);     
        	}
        },
		fetchTransactionStats: function () {
			$(TRANSACTIONS_SPINNER).show();
			dashboardHandler.fetchTransactionStats(_this.generateParams('transactions'));       	
        },
        fetchTransactionStatsSuccess: function(result){
			$(TRANSACTIONS_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(TRANSACTIONS_RESULT_CONTAINER).hide();
				$(TRANSACTIONS_ERROR_TEXT).text("No data available");
				$(TRANSACTIONS_ERROR_TEXT).show();				
			}else{
				$(TRANSACTIONS_RESULT_CONTAINER).show();
				$(TRANSACTIONS_ERROR_TEXT).hide();   	

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1, a: parseInt(result[value][0]*30),b: parseInt(result[value][1]*30),c: parseInt(result[value][2]*30)});
				});
				if(transactionsGraph == null) {
					transactionsGraph = Morris.Line({
						element: 'transactions-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['a','b','c'],
						labels: [],
						hideHover: 'auto',
						pointSize: '0.0',
						resize:true,
						yLabelFormat: function(y){
							return y;
						},
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x.getTime(),'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "#Aborts : " + common.formatNumberWithCommas(row.a) + 
							  	" <br/>#Begins : " + common.formatNumberWithCommas(row.b) + 
							  	" <br/>#Commits : " + common.formatNumberWithCommas(row.c) + 
							  	"<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					transactionsGraph.setData(seriesData);
				}
			}
		},
		fetchTransactionStatsError: function(jqXHR, res, error){
			$(TRANSACTIONS_SPINNER).hide();
        	$(TRANSACTIONS_RESULT_CONTAINER).hide();
        	$(TRANSACTIONS_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(TRANSACTIONS_ERROR_TEXT).text(jqXHR.responseText);     
        	}
		},	        
		fetchSummaryMetricSuccess: function(result){
			var keys = Object.keys(result.data);
			var metricConfig = chartConfig[result.metricName];
			$(metricConfig.spinner).hide();
			
			if(keys.length == 0){
				$('#'+metricConfig.graphcontainer).hide();
				$(metricConfig.errorcontainer).text("No data available");
				$(metricConfig.errorcontainer).show();				
			}else{
				$('#'+metricConfig.graphcontainer).show();
				$(metricConfig.errorcontainer).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*metricConfig.xtimemultiplier, y: result.data[value]});
				});
				var graph = renderedCharts[result.metricName];
				
				if(graph == null) {
					graph = Morris.Line({
						element: metricConfig.graphcontainer,
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						yLabelFormat: function(y){
							if(metricConfig.yLabelFormat){
								return metricConfig.yLabelFormat(y);
							}
							return y;
						},						
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x.getTime(),'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
		        			var toolTipText = "";
							if(metricConfig.ylabel){
								toolTipText += metricConfig.ylabel + " : ";
							}
							if(metricConfig.yvalformatter){
								toolTipText += metricConfig.yvalformatter(row.y.toFixed(2));
							}else{
								toolTipText += row.y.toFixed(2);
							}
							if(metricConfig.ylabelunit){
								toolTipText += metricConfig.ylabelunit;
							}
							toolTipText += "<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							  return toolTipText
							}
					});
					renderedCharts[result.metricName] = graph;
				}else{
					graph.setData(seriesData);
				}
			}
		},
		fetchSummaryMetricError: function(jqXHR, res, error){
			if(jqXHR.metricName){
				var metricConfig = chartConfig[result.metricName];
				$(metricConfig.spinner).hide();
	        	$('#'+metricConfig.graphcontainer).hide();
	        	$(metricConfig.errorcontainer).show();
	        	if (jqXHR.responseText) {
	    			$(metricConfig.errorcontainer).text(jqXHR.responseText);     
	        	}				
			}
		},		

        showErrorMessage: function (jqXHR) {
        	_this.hideLoading();
        }  
    });
    

    return DashboardView;
});
