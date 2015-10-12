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
	    
	    CANARY_SPINNER = '#canarySpinner',
	    CANARY_RESULT_CONTAINER = '#canary-response-chart',
	    CANARY_ERROR_TEXT = '#canary-error-text',
	    
	    TRANSACTIONS_SPINNER = '#transactionsSpinner',
	    TRANSACTIONS_RESULT_CONTAINER = '#transactions-response-chart',
	    TRANSACTIONS_ERROR_TEXT = '#transactions-error-text',
	    
	    DISKSPACEUSED_SPINNER = '#spaceusedSpinner',
	    DISKSPACEUSED_CONTAINER = '#spaceused-chart',
	    DISKSPACEUSED_ERROR_TEXT = '#spaceused-error-text',
	    
	    IOWAIT_SPINNER = '#iowaitSpinner',
	    IOWAIT_CONTAINER = '#iowait-chart',
	    IOWAIT_ERROR_TEXT = '#iowait-error-text',
	    
	    GCTIME_SPINNER = '#gctimeSpinner',
	    GCTIME_CONTAINER = '#gctime-chart',
	    GCTIME_ERROR_TEXT = '#gctime-error-text',
	    
	    MEMSTORE_SPINNER = '#memStoreSpinner',
	    MEMSTORE_CONTAINER = '#memStore-chart',
	    MEMSTORE_ERROR_TEXT = '#memStore-error-text',
	    
	    RSRVR_MEMORY_SPINNER = '#regionserverMemSpinner',
	    RSRVR_MEMORY_CONTAINER = '#regionserverMem-chart',
	    RSRVR_MEMORY_ERROR_TEXT = '#regionserverMem-error-text',
	    
	    REFRESH_ACTION = '#refreshAction',
	    OPEN_FILTER = '#openFilter';
    
    var cpuGraph = null, 
	    diskReadsGraph = null, 
	    diskWritesGraph=null, 
	    getOpsGraph = null, 
	    canaryGraph = null,
	    transactionsGraph = null,
	    iowaitsGraph = null,
	    diskSpaceUsedGraph = null,
	    memStoreGraph = null,
	    regionserverMemGraph = null,
	    gctimeGraph = null;
    
    var servicesTable = null, nodesTable = null;
    
    var DashboardView = BaseView.extend({
    	template:  _.template(DashboardT),
    	
		doInit: function (){
			
			_this = this;
			refreshTimer.init();
			timeRangeView.init();
		
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$(NODES_ERROR_TEXT).hide();

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.on(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.on(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);
			dashboardHandler.on(dashboardHandler.DISKSPACEUSED_SUCCESS, this.fetchUsedDiskSpaceSuccess); 
			dashboardHandler.on(dashboardHandler.DISKSPACEUSED_ERROR, this.fetchUsedDiskSpaceError);
			dashboardHandler.on(dashboardHandler.IOWAIT_SUCCESS, this.fetchIOWaitsSuccess); 
			dashboardHandler.on(dashboardHandler.IOWAIT_ERROR, this.fetchIOWaitsError);
			dashboardHandler.on(dashboardHandler.GCTIME_SUCCESS, this.fetchGCTimeSuccess); 
			dashboardHandler.on(dashboardHandler.GCTIME_ERROR, this.fetchGCTimeError);
			dashboardHandler.on(dashboardHandler.RSRVR_MEMORY_SUCCESS, this.fetchRegionServerMemoryUsageSuccess); 
			dashboardHandler.on(dashboardHandler.RSRVR_MEMORY_ERROR, this.fetchRegionServerMemoryUsageError);
			dashboardHandler.on(dashboardHandler.MEMSTORE_SUCCESS, this.fetchMemStoreSizeSuccess); 
			dashboardHandler.on(dashboardHandler.MEMSTORE_ERROR, this.fetchMemStoreSizeError);
			
			
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
			dashboardHandler.on(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.on(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.on(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);
			dashboardHandler.on(dashboardHandler.DISKSPACEUSED_SUCCESS, this.fetchUsedDiskSpaceSuccess); 
			dashboardHandler.on(dashboardHandler.DISKSPACEUSED_ERROR, this.fetchUsedDiskSpaceError);
			dashboardHandler.on(dashboardHandler.IOWAIT_SUCCESS, this.fetchIOWaitsSuccess); 
			dashboardHandler.on(dashboardHandler.IOWAIT_ERROR, this.fetchIOWaitsError);
			dashboardHandler.on(dashboardHandler.GCTIME_SUCCESS, this.fetchGCTimeSuccess); 
			dashboardHandler.on(dashboardHandler.GCTIME_ERROR, this.fetchGCTimeError);
			dashboardHandler.on(dashboardHandler.RSRVR_MEMORY_SUCCESS, this.fetchRegionServerMemoryUsageSuccess); 
			dashboardHandler.on(dashboardHandler.RSRVR_MEMORY_ERROR, this.fetchRegionServerMemoryUsageError);
			dashboardHandler.on(dashboardHandler.MEMSTORE_SUCCESS, this.fetchMemStoreSizeSuccess); 
			dashboardHandler.on(dashboardHandler.MEMSTORE_ERROR, this.fetchMemStoreSizeError);
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
			dashboardHandler.off(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.off(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			dashboardHandler.off(dashboardHandler.TRANSACTION_STATS_SUCCESS, this.fetchTransactionStatsSuccess); 
			dashboardHandler.off(dashboardHandler.TRANSACTION_STATS_ERROR, this.fetchTransactionStatsError);
			dashboardHandler.off(dashboardHandler.DISKSPACEUSED_SUCCESS, this.fetchUsedDiskSpaceSuccess); 
			dashboardHandler.off(dashboardHandler.DISKSPACEUSED_ERROR, this.fetchUsedDiskSpaceError);
			dashboardHandler.off(dashboardHandler.IOWAIT_SUCCESS, this.fetchIOWaitsSuccess); 
			dashboardHandler.off(dashboardHandler.IOWAIT_ERROR, this.fetchIOWaitsError);
			dashboardHandler.off(dashboardHandler.GCTIME_SUCCESS, this.fetchGCTimeSuccess); 
			dashboardHandler.off(dashboardHandler.GCTIME_ERROR, this.fetchGCTimeError);
			dashboardHandler.off(dashboardHandler.RSRVR_MEMORY_SUCCESS, this.fetchRegionServerMemoryUsageSuccess); 
			dashboardHandler.off(dashboardHandler.RSRVR_MEMORY_ERROR, this.fetchRegionServerMemoryUsageError);
			dashboardHandler.off(dashboardHandler.MEMSTORE_SUCCESS, this.fetchMemStoreSizeSuccess); 
			dashboardHandler.off(dashboardHandler.MEMSTORE_ERROR, this.fetchMemStoreSizeError);
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
        refreshPage: function() {
        	timeRangeView.updateFilter();
        	_this.fetchServices();
        	_this.fetchNodes();
        	_this.fetchCanaryResponse();
        	_this.fetchTransactionStats();
        	_this.fetchUsedDiskSpace();
        	_this.fetchIOWaits();
        	_this.fetchMemStoreSize();
        	_this.fetchRegionServerMemoryUsage();
        	_this.fetchGCTime();
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
        fetchCanaryResponse: function () {
			$(CANARY_SPINNER).show();
			dashboardHandler.fetchCanaryResponse();       	
        },
        fetchCanarySuccess: function(result){
			$(CANARY_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(CANARY_RESULT_CONTAINER).hide();
				$(CANARY_ERROR_TEXT).text("No data available");
				$(CANARY_ERROR_TEXT).show();				
			}else{
				$(CANARY_RESULT_CONTAINER).show();
				$(CANARY_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1000, y: result[value]});
				});
				if(canaryGraph == null) {
					canaryGraph = Morris.Line({
						element: 'canary-response-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "Canary Response Time : " + common.formatNumberWithCommas(row.y) + " msec <br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					canaryGraph.setData(seriesData);
				}
			}
		},
		fetchCanaryError: function(jqXHR, res, error){
			$(CANARY_SPINNER).hide();
        	$(CANARY_RESULT_CONTAINER).hide();
        	$(CANARY_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(CANARY_ERROR_TEXT).text(jqXHR.responseText);     
        	}
		},
		fetchTransactionStats: function () {
			$(TRANSACTIONS_SPINNER).show();
			dashboardHandler.fetchTransactionStats();       	
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
						element: 'transactions-response-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['a','b','c'],
						labels: [],
						hideHover: 'auto',
						pointSize: '0.0',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
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
		fetchIOWaits: function () {
			$(IOWAIT_SPINNER).show();
			dashboardHandler.fetchIOWaits();       	
        },
        fetchIOWaitsSuccess: function(result){
			$(IOWAIT_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(IOWAIT_CONTAINER).hide();
				$(IOWAIT_ERROR_TEXT).text("No data available");
				$(IOWAIT_ERROR_TEXT).show();				
			}else{
				$(IOWAIT_CONTAINER).show();
				$(IOWAIT_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1000, y: result[value]});
				});
				if(iowaitsGraph == null) {
					iowaitsGraph = Morris.Line({
						element: 'iowait-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "IO Waits : " + row.y.toFixed(2) + "<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					iowaitsGraph.setData(seriesData);
				}
			}
		},
		fetchIOWaitsError: function(jqXHR, res, error){
			$(IOWAIT_SPINNER).hide();
        	$(IOWAIT_CONTAINER).hide();
        	$(IOWAIT_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(IOWAIT_ERROR_TEXT).text(jqXHR.responseText);     
        	}
		},
		fetchUsedDiskSpace: function () {
			$(DISKSPACEUSED_SPINNER).show();
			dashboardHandler.fetchUsedDiskSpace();       	
        },
        fetchUsedDiskSpaceSuccess: function(result){
			$(DISKSPACEUSED_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(DISKSPACEUSED_CONTAINER).hide();
				$(DISKSPACEUSED_ERROR_TEXT).text("No data available");
				$(DISKSPACEUSED_ERROR_TEXT).show();				
			}else{
				$(DISKSPACEUSED_CONTAINER).show();
				$(DISKSPACEUSED_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1, y: result[value]});
				});
				if(diskSpaceUsedGraph == null) {
					diskSpaceUsedGraph = Morris.Line({
						element: 'spaceused-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "Disk Space Used : " + row.y.toFixed(2) + "%<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					diskSpaceUsedGraph.setData(seriesData);
				}
			}
		},
		fetchUsedDiskSpaceError: function(jqXHR, res, error){
			$(DISKSPACEUSED_SPINNER).hide();
        	$(DISKSPACEUSED_CONTAINER).hide();
        	$(DISKSPACEUSED_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(DISKSPACEUSED_ERROR_TEXT).text(jqXHR.responseText);     
        	}
		},
		fetchGCTime: function(){
			$(GCTIME_SPINNER).show();
			dashboardHandler.fetchGCTime(); 			
		},
		fetchGCTimeSuccess: function(result){
			$(GCTIME_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(GCTIME_CONTAINER).hide();
				$(GCTIME_ERROR_TEXT).text("No data available");
				$(GCTIME_ERROR_TEXT).show();				
			}else{
				$(GCTIME_CONTAINER).show();
				$(GCTIME_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1, y: result[value]});
				});
				if(gctimeGraph == null) {
					gctimeGraph = Morris.Line({
						element: 'gctime-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "GC Time : " + row.y.toFixed(2) + "msec<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					gctimeGraph.setData(seriesData);
				}
			}
		},
		fetchGCTimeError: function(jqXHR, res, error){
			$(GCTIME_SPINNER).hide();
        	$(GCTIME_CONTAINER).hide();
        	$(GCTIME_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(GCTIME_ERROR_TEXT).text(jqXHR.responseText);     
        	}			
		},
		fetchRegionServerMemoryUsage: function(){
			$(RSRVR_MEMORY_SPINNER).show();
			dashboardHandler.fetchRegionServerMemoryUsage(); 				
		},
		fetchRegionServerMemoryUsageSuccess: function(result){
			$(RSRVR_MEMORY_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(RSRVR_MEMORY_CONTAINER).hide();
				$(RSRVR_MEMORY_ERROR_TEXT).text("No data available");
				$(RSRVR_MEMORY_ERROR_TEXT).show();				
			}else{
				$(RSRVR_MEMORY_CONTAINER).show();
				$(RSRVR_MEMORY_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1, y: result[value]});
				});
				if(regionserverMemGraph == null) {
					regionserverMemGraph = Morris.Line({
						element: 'regionserverMem-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "Memory Usage : " + row.y.toFixed(2) + "MB<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					regionserverMemGraph.setData(seriesData);
				}
			}
		},
		fetchRegionServerMemoryUsageError: function(jqXHR, res, error){
			$(RSRVR_MEMORY_SPINNER).hide();
        	$(RSRVR_MEMORY_CONTAINER).hide();
        	$(RSRVR_MEMORY_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(RSRVR_MEMORY_ERROR_TEXT).text(jqXHR.responseText);     
        	}			
		},		
		fetchMemStoreSize: function(){
			$(MEMSTORE_SPINNER).show();
			dashboardHandler.fetchMemStoreSize(); 				
		},
		fetchMemStoreSizeSuccess: function(result){
			$(MEMSTORE_SPINNER).hide();
			var keys = Object.keys(result);
			
			if(keys.length == 0){
				$(MEMSTORE_CONTAINER).hide();
				$(MEMSTORE_ERROR_TEXT).text("No data available");
				$(MEMSTORE_ERROR_TEXT).show();				
			}else{
				$(MEMSTORE_CONTAINER).show();
				$(MEMSTORE_ERROR_TEXT).hide();

				var seriesData = [];
				$.each(keys, function(index, value){
					seriesData.push({x: value*1, y: result[value]});
				});
				if(memStoreGraph == null) {
					memStoreGraph = Morris.Line({
						element: 'memStore-chart',
						data: seriesData,
						xkey:'x',
						ykeys:['y'],
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						yLabelFormat: function(y){
							return common.convertToMB(y);
						},
						xLabelFormat: function(x){
							return common.toServerLocalDateFromUtcMilliSeconds(x,'HH:mm');
						},
						hoverCallback: function (index, options, content, row) {
							  return "Memstore Size : " + common.convertToMB(row.y) + "MB<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
							}
					});
				}else{
					memStoreGraph.setData(seriesData);
				}
			}
		},
		fetchMemStoreSizeError: function(jqXHR, res, error){
			$(MEMSTORE_SPINNER).hide();
        	$(MEMSTORE_CONTAINER).hide();
        	$(MEMSTORE_ERROR_TEXT).show();
        	if (jqXHR.responseText) {
    			$(MEMSTORE_ERROR_TEXT).text(jqXHR.responseText);     
        	}			
		},
		fetchCPUData: function () {
			_this.showLoading();
			dashboardHandler.fetchCPUData();
		},

		displayCPUGraph: function (result){
			_this.hideLoading();
			var keys = Object.keys(result);
			var seriesData = [];
			$.each(keys, function(index, value){
				seriesData.push({x: value*1000, y: result[value]});
			});
			if(cpuGraph == null) {
				cpuGraph = Morris.Line({
					element: 'cpu-area-chart',
					data: seriesData,
					xkey:'x',
					ykeys:['y'],
					labels: [],
					hideHover: 'auto',
					hoverCallback: function (index, options, content, row) {
						  return "CPU Usage : " + row.y + "<br/>Time : " + row.x;
						}
				});
			}else{
				cpuGraph.setData(seriesData);
			}
		},
		
		fetchDiskReads: function () {
			$('#readLoadingImg').show();
			dashboardHandler.fetchDiskReads();
		},
		fetchDiskReadsSuccess: function(result){
			$('#readLoadingImg').hide();
			var keys = Object.keys(result);
			var seriesData = [];
			$.each(keys, function(index, value){
				seriesData.push({x: value*1000, y: result[value]});
			});
			if(diskReadsGraph == null) {
				diskReadsGraph = Morris.Line({
					element: 'read-requests-chart',
					data: seriesData,
					xkey:'x',
					ykeys:['y'],
					labels: [],
					hideHover: 'auto',
					resize:true,
					hoverCallback: function (index, options, content, row) {
						  return "Disk Reads : " + row.y + "<br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
						}
				});
			}else{
				diskReadsGraph.setData(seriesData);
			}			
		},
		fetchDiskReadsError: function(){
			$('#readLoadingImg').hide();
		},
		
		fetchDiskWrites: function () {
			$('#writesLoadingImg').hide();
			dashboardHandler.fetchDiskWrites();
		},
		fetchDiskWritesSuccess: function(result){
			$('#writesLoadingImg').hide();
			var keys = Object.keys(result);
			var seriesData = [];
			$.each(keys, function(index, value){
				seriesData.push({x: value*1000, y: result[value]});
			});
			if(diskWritesGraph == null) {
				diskWritesGraph = Morris.Line({
					element: 'write-requests-chart',
					data: seriesData,
					xkey:'x',
					ykeys:['y'],
					labels: [],
					hideHover: 'auto',
					hoverCallback: function (index, options, content, row) {
						  return "Disk Writes : " + row.y.toFixed(2) + "<br/>Time : " + row.x;
						}
				});
			}else{
				diskWritesGraph.setData(seriesData);
			}
		},
		fetchDiskWritesError: function(){
			$('#writesLoadingImg').hide();
		},
	
		fetchGetOps: function () {
			$('#thruputLoadingImg').hide();
			dashboardHandler.fetchGetOps();
		},
		fetchGetOpsSuccess: function(result){
			$('#thruputLoadingImg').hide();
			var keys = Object.keys(result);
			var seriesData = [];
			$.each(keys, function(index, value){
				seriesData.push({x: value*1000, y: result[value]});
			});
			if(getOpsGraph == null) {
				getOpsGraph = Morris.Line({
					element: 'throughput-chart',
					data: seriesData,
					xkey:'x',
					ykeys:['y'],
					labels: [],
					hideHover: 'auto',
					hoverCallback: function (index, options, content, row) {
						  return "Throughput : " + row.y.toFixed(2) + "<br/>Time : " + row.x;
						}
				});
			}else{
				getOpsGraph.setData(seriesData);
			}
		},
		fetchGetOpsError: function(jqXHR, res, error){
			$('#thruputLoadingImg').hide();
		},

        showErrorMessage: function (jqXHR) {
        	_this.hideLoading();
        }  
    });
    

    return DashboardView;
});
