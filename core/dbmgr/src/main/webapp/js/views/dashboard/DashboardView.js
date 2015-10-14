//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

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

	REFRESH_ACTION = '#refreshAction',
	OPEN_FILTER = '#openFilter';

	var transactionsGraph = null;

	var renderedCharts = {};

	var chartConfig = null;
	var transConfig = null;

	var servicesTable = null, nodesTable = null;

	var DashboardView = BaseView.extend({
		template:  _.template(DashboardT),

		doInit: function (){

			_this = this;
			refreshTimer.init();
			timeRangeView.init();
			chartConfig =  {
					canary:{
						chartType: "Line",
						xtimemultiplier: 1000, //sometimes time don't comeback as msecs. so we need a multiplier
						ylabels: "Canary Response Time", //Used in tooltip
						yunit: "msec", //Units displayed in tooltip
						yvalformatter: common.formatNumberWithComma, //callback to format Y value
						spinner:"#canary-spinner",  //div element for spinner
						graphcontainer:"canary-chart",  //element for graph container
						errorcontainer:"#canary-error-text", //element for error text container
					},
					transactions:{
						chartType: "Line",
						xtimemultiplier: 1,
						deltamultiplier: 300, //For delta rate/sec counters, multiply by seconds interval to get real delta
						ylabels: ["#Aborts", "#Begins", "#Commits"], // 3series, so 3 labels
						yunit: "",
						ydecimals: 0,
						yvalformatter: common.formatNumberWithComma,
						spinner:"#transactions-spinner", 
						graphcontainer:"transactions-chart", 
						errorcontainer:"#transactions-error-text",						
					},
					iowaits:{
						chartType: "Line",
						xtimemultiplier: 1000,
						ylabels: "IO Waits",
						yunit: "",
						spinner:"#iowaits-spinner",
						graphcontainer:"iowaits-chart",
						errorcontainer:"#iowaits-error-text"
					},
					useddiskspace:{
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: "Disk Space Used",
						yunit: "%",
						spinner:"#useddiskspace-spinner",
						graphcontainer:"useddiskspace-chart",
						errorcontainer:"#useddiskspace-error-text"
					},
					memstoresize:{
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: "Memstore Size",
						yunit: "MB",
						ylabelsFormat: common.convertToMB,
						yvalformatter: common.convertToMB,
						spinner:"#memstoresize-spinner",
						graphcontainer:"memstoresize-chart",
						errorcontainer:"#memstoresize-error-text"
					},
					jvmgctime:{
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: "GC Time",
						yunit: "msec",
						spinner:"#jvmgctime-spinner",
						graphcontainer:"jvmgctime-chart",
						errorcontainer:"#jvmgctime-error-text"
					},
					regionservermemory:{
						chartType: "Area",
						xtimemultiplier: 1,
						ylabels: ["Max. Memory", "Avg. Memory", "Min. Memory"],
						yunit: "MB",
						spinner:"#regionservermemory-spinner",
						graphcontainer:"regionservermemory-chart",
						errorcontainer:"#regionservermemory-error-text"
					}
			};

			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$(NODES_ERROR_TEXT).hide();

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 

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

			dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
			dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);

			$(REFRESH_ACTION).off('click', this.refreshPage);

			refreshTimer.eventAgg.off(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.off(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);	
			timeRangeView.eventAgg.off(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);
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

			$.each(Object.getOwnPropertyNames(chartConfig), function(k, v){
				$(chartConfig[v].spinner).show();
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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table dbmgr-dashboard-table dt-responsive" id="services-results"></table>';
				$(SERVICES_RESULT_CONTAINER).html( sb );

				var aoColumns = [];

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					if(obj.title == 'PROCESS'){
						obj.title = 'Service'
					}
					aoColumns.push(obj);
				});

				var obj = new Object();
				obj.title = "Status";
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
					                 { "aTargets": [ 0 ], "sTitle": "SERVICE", "mData": 0, "sWidth":"30px",
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
					                	 "sWidth":"20px",
					                	 "mRender": function ( data, type, full ) {
					                		 if (type === 'display') {
					                			 if(data == 'ERROR'){
					                				 return '<button type="button" class="btn btn-danger btn-circle btn-small dbmgr-status-btn" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
					                				 'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0")+'"><i class="fa fa-times"></i></button>';
					                			 }
					                			 if(data == 'WARN'){
					                				 return '<button type="button" class="btn btn-warning btn-circle btn-small dbmgr-status-btn" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
					                				 'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0")+'"><i class="fa fa-warning"></i></button>';
					                			 }
					                			 return '<button type="button" class="btn btn-success btn-circle btn-small dbmgr-status-btn" data-trigger="focus" data-toggle="tooltip" data-placement="left" title="'+
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
					                		 container: "body",
					                		 html: true
					                	 }).css('overflow','auto');

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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table  dbmgr-dashboard-table dt-responsive" id="nodes-results"></table>';
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
					dom: 't<"clear">iTf',
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
					                				 return '<button type="button" class="btn btn-danger btn-circle btn-small dbmgr-status-btn"><i class="fa fa-times"></i></button>';
					                			 }
					                			 return '<button type="button" class="btn btn-success btn-circle btn-small dbmgr-status-btn"><i class="fa fa-check"></i></button>';
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
				var ykeys = [];

				var ykeys = [];
				$.each(result.data[keys[0]], function(i, v){
					ykeys.push('y'+i);
				});
				$.each(keys, function(index, value){
					var rowData = {x: metricConfig.xtimemultiplier? value*metricConfig.xtimemultiplier: value};
					$.each(result.data[value], function(i, v){
						if(metricConfig.deltamultiplier){
							rowData[ykeys[i]] = v * metricConfig.deltamultiplier;	
						}else{
							rowData[ykeys[i]] = v;
						}
					});
					//seriesData.push({x: value*1, a: parseInt(result[value][0]*30),b: parseInt(result[value][1]*30),c: parseInt(result[value][2]*30)});
					seriesData.push(rowData);
				});

				var graph = renderedCharts[result.metricName];
				
				var options = {
						element: metricConfig.graphcontainer,
						data: seriesData,
						lineWidth:2,
						xkey:'x',
						ykeys:ykeys,
						labels: [],
						pointSize: '0.0',
						hideHover: 'auto',
						resize:true,
						ylabelsFormat: function(y){
							if(metricConfig.ylabelsFormat){
								return metricConfig.ylabelsFormat(y);
							}
							return y;
						},						
						hoverCallback: function (index, options, content, row) {
							var toolTipText = "";
							var nDecimals = 2;
							if(metricConfig.ydecimals != null){
								nDecimals = metricConfig.ydecimals;
							}
							if(metricConfig.ylabels){
								var yLabelArray = [];
								if($.isArray(metricConfig.ylabels)){
									yLabelArray = metricConfig.ylabels;
								}else{
									yLabelArray.push(metricConfig.ylabels);
								}
								$.each(yLabelArray, function(i, v){
									toolTipText += yLabelArray[i] + " : ";
									if(metricConfig.yvalformatter){

										toolTipText += metricConfig.yvalformatter(row['y'+i].toFixed(nDecimals));
									}else{
										toolTipText += row['y'+i].toFixed(nDecimals);
									}
									if(metricConfig.yunit){
										toolTipText += metricConfig.yunit;
									}
									toolTipText += '<br>';
								});
							}
							return toolTipText;
						}
					};

				if(graph == null) {
					if(metricConfig.chartType == 'Area'){
						options.behaveLikeLine = true;
						graph = Morris.Area(options);
					}else{
						graph = Morris.Line(options);
					}
					
					renderedCharts[result.metricName] = graph;
				}else{
					graph.setData(seriesData);
				}
			}
		},
		fetchSummaryMetricError: function(jqXHR, res, error){
			if(jqXHR.metricName){
				var metricConfig = chartConfig[jqXHR.metricName];
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
