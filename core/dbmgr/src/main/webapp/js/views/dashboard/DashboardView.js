//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/dashboard.html',
        'handlers/DashboardHandler',
        'handlers/ServerHandler',
        'jquery',
        'common',
        'moment',
        'views/RefreshTimerView',
        'views/TimeRangeView',
        'datatables.net',
        'datatables.net-bs',
        'responsivetable',
        'datatables.net-buttons',
        'buttonsprint',
        'buttonshtml',
        'pdfmake',
        'flot',
        'flottime',
        'flotcanvas',
        'flotcrosshair',
        'flotaxislabels'
        ], function (BaseView, DashboardT, dashboardHandler, serverHandler, $, common, moment, refreshTimer, timeRangeView) {
	'use strict';

	var _this = null;
	var SERVICES_SPINNER = '#services-spinner',
	SERVICES_RESULT_CONTAINER = '#services-result-container',
	SERVICES_ERROR_TEXT = '#services-error-text',

	NODES_SPINNER = '#nodes-spinner',
	NODES_RESULT_CONTAINER = '#nodes-result-container',
	GRID_DRILLDOWN_CONTAINER = '#grid-drilldown-container',
	NODES_ERROR_TEXT = '#nodes-error-text',

	CANARY_DRILLDOWN_BTN = '#canary-drilldown-btn',
	TRANSACTIONS_DRILLDOWN_BTN = '#transactions-drilldown-btn',
	IOWAITS_DRILLDOWN_BTN = '#iowaits-drilldown-btn',
	DISK_SPACE_DRILLDOWN_BTN = '#useddiskspace-drilldown-btn',
	JVMGC_DRILLDOWN_BTN = '#jvmgctime-drilldown-btn',
	RSERVER_MEMORY_DRILLDOWN_BTN = '#regionservermemory-drilldown-btn',
	MEMSTORE_DRILLDOWN_BTN = '#memstoresize-drilldown-btn',
	CPULOAD_DRILLDOWN_BTN = '#cpuload-drilldown-btn',
	FREEMEM_DRILLDOWN_BTN = '#freememory-drilldown-btn',
	NETWORKIO_DRILLDOWN_BTN = '#network-io-drilldown-btn',
	NODES_DRILLDOWN_BTN = '#nodes-drilldown-btn',

	DRILLDOWN_DIALOG = '#metricsDialog',
	DRILLDOWN_TITLE = '#metricsDialogLabel',
	DRILLDOWN_SPINNER = '#metrics-drilldown-spinner',
	DRILLDOWN_CHART_CONTAINER = '#metrics-drilldown-container',
	DRILLDOWN_CHART = '#metrics-drilldown-chart',
	DRILLDOWN_ERROR_CONTAINER= '#metrics-drilldown-error-text',
	DRILLDOWN_LEGEND = '#metrics-drilldown-legend',
	DRILLDOWN_METRICNAME = '#metric-name-holder',
	DRILLDOWN_SERIES_CONTAINER = '#metric-series-select-container',
	SERIES_SELECTOR = '#seriesSelect',
	FILTER_DIALOG = '#filterDialog',
	FILTER_FORM = '#filter-form',
	FILTER_APPLY_BUTTON = "#filterApplyButton",
	FILTER_START_TIME = '#filter-start-time',
	FILTER_END_TIME = '#filter-end-time',
	START_TIME_PICKER = '#startdatetimepicker',
	END_TIME_PICKER = '#enddatetimepicker',
	FILTER_TIME_RANGE = '#timeRange',
	REFRESH_ACTION = '#refreshAction',
	OPEN_FILTER = '#openFilter';

	var transactionsGraph = null;

	var renderedCharts = {};
	var renderedFlotCharts = {};
	var chartsData = {};
	var resizeTimer = null;
	var drillDownChart = {};
	var chartConfig = null;
	var transConfig = null;

	var servicesTable = null, nodesTable = null;
	var timeinterval = 0;
	var lastUsedTimeRange = null;
	var nodesStatusData = null;
	//var graphColors = ['#3c8dbc','#00c0ef','#FE2E9A','#04B404','#0096D6','#F5AC50'];
	//var graphColors = ["#7cb5ec","#434348","#90ed7d","#f7a35c","#8085e9","#f15c80","#e4d354","#2b908f","#f45b5b","#91e8e1"];
	var graphColors = ['#3c8dbc','#00c0ef',"#90ed7d","#f7a35c","#8085e9","#f15c80","#e4d354","#2b908f","#f45b5b","#91e8e1"];

	var DashboardView = BaseView.extend({
		template:  _.template(DashboardT),

		initVariables: function (){
			renderedCharts = {};
			chartsData = {};
			chartConfig = null;
			transConfig = null;
			servicesTable = null;
			nodesTable = null;
			timeinterval = 0;
		},
		doInit: function (){

			_this = this;
			_this.initVariables();
			refreshTimer.init();
			timeRangeView.init();
			chartConfig =  {
					canary:{
						chartTitle: "Canary Response Time",
						chartType: "Line",
						xtimemultiplier: 1000, //sometimes time don't comeback as msecs. so we need a multiplier
						ylabels: ["Connect Time","DDL Time","Write Time", "Read Time"], //Used in tooltip
						yunit: "msec", //Units displayed in tooltip
						yvalformatter: common.formatNumberWithComma, //callback to format Y value
						spinner:"#canary-spinner",  //div element for spinner
						graphcontainer:"canary-chart",  //element for graph container
						errorcontainer:"#canary-error-text", //element for error text container
					},
					transactions:{
						chartTitle: "Transaction Counts",
						chartType: "Line",
						xtimemultiplier: 1,
						deltamultiplier: 300, //For delta rate/sec counters, multiply by seconds interval to get real delta
						ylabels: ["#Aborts", "#Begins", "#Commits"], // 3series, so 3 labels
						yunit: "",
						ydecimals: 0,
						yvalround: true,
						yvalformatter: common.formatNumberWithComma,
						spinner:"#transactions-spinner", 
						graphcontainer:"transactions-chart", 
						errorcontainer:"#transactions-error-text"						
					},
					iowaits:{
						chartTitle: "IO Waits",
						chartType: "Line",
						xtimemultiplier: 1000,
						ylabels: ["IO Waits"],
						yunit: "",
						spinner:"#iowaits-spinner",
						graphcontainer:"iowaits-chart",
						errorcontainer:"#iowaits-error-text"
					},
					useddiskspace:{
						chartTitle: "Disk Space Usage",
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: ["Avg. Disk Usage"],
						yunit: "%",
						ymax: 100,
						spinner:"#useddiskspace-spinner",
						graphcontainer:"useddiskspace-chart",
						errorcontainer:"#useddiskspace-error-text"
					},
					memstoresize:{
						chartTitle: "Memstore Size",
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: ["Memstore Size"],
						yunit: "MB",
						yLabelFormat: common.convertToMB,
						yvalformatter: common.convertToMB,
						spinner:"#memstoresize-spinner",
						graphcontainer:"memstoresize-chart",
						errorcontainer:"#memstoresize-error-text"
					},
					jvmgctime:{
						chartTitle: "Regionserver JVM GC Time",
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: ["GC Time"],
						yunit: "msec",
						spinner:"#jvmgctime-spinner",
						graphcontainer:"jvmgctime-chart",
						errorcontainer:"#jvmgctime-error-text"
					},
					regionservermemory:{
						chartTitle: "Regionserver Memory Usage",
						chartType: "Line",
						xtimemultiplier: 1,
						ylabels: ["Avg. Memory"],
						yunit: "MB",
						spinner:"#regionservermemory-spinner",
						graphcontainer:"regionservermemory-chart",
						errorcontainer:"#regionservermemory-error-text"
					},
					cpuload:{
						chartTitle: "CPU Load Avg.",
						chartType: "Line",
						xtimemultiplier: 1000,
						ylabels: ["CPU Load Avg. 15min"],
						yLabelFormat: common.fortmat2Decimals,
						spinner:"#cpuload-spinner",
						graphcontainer:"cpuload-chart",
						errorcontainer:"#cpuload-error-text"
					},
					freememory:{
						chartTitle: "Free Memory",
						chartType: "Line",
						xtimemultiplier: 1000,
						ylabels: ["Free Memory"],
						yunit: "MB",
						yLabelFormat: common.convertToMB,
						yvalformatter: common.convertToMB,
						spinner:"#freememory-spinner",
						graphcontainer:"freememory-chart",
						errorcontainer:"#freememory-error-text"
					},
					networkio:{
						chartTitle: "Network IO",
						chartType: "Line",
						xtimemultiplier: 1000,
						ylabels: ["Network In", "Network Out"],
						yunit: "MB",
						yLabelFormat: common.convertToMB,
						yvalformatter: common.convertToMB,
						spinner:"#network-io-spinner",
						graphcontainer:"network-io-chart",
						errorcontainer:"#network-io-error-text"
					}

			};

			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$(NODES_ERROR_TEXT).hide();

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 

			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.on(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);
			timeRangeView.eventAgg.on(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);
			refreshTimer.setRefreshInterval(0.5);
			timeRangeView.setTimeRange(1);
			$(window).on('resize', this.onResize);

			if(common.isEnterprise()){
				$('.dbmgr-ent').show();
				$(CANARY_DRILLDOWN_BTN).on('click', this.canaryDrillDown);
				$(TRANSACTIONS_DRILLDOWN_BTN).on('click', this.transactionsDrillDown);
				$(IOWAITS_DRILLDOWN_BTN).on('click',this.iowaitsDrillDown);
				$(DISK_SPACE_DRILLDOWN_BTN).on('click',this.diskspaceDrillDown);
				$(JVMGC_DRILLDOWN_BTN).on('click',this.jvmGCDrillDown);
				$(RSERVER_MEMORY_DRILLDOWN_BTN).on('click',this.rserverMemoryDrillDown);
				$(MEMSTORE_DRILLDOWN_BTN).on('click',this.memStoreDrillDown);
				$(CPULOAD_DRILLDOWN_BTN).on('click',this.cpuLoadDrillDown);
				$(FREEMEM_DRILLDOWN_BTN).on('click',this.freeMemoryDrillDown);
				$(NETWORKIO_DRILLDOWN_BTN).on('click',this.networkIODrillDown);
				$(NODES_DRILLDOWN_BTN).on('click',this.nodeStatusDrillDown);

				$.each(Object.getOwnPropertyNames(chartConfig), function(k, v){

					$("#"+chartConfig[v].graphcontainer).bind("plothover", function (event, pos, item) {

						var cPlot = renderedFlotCharts[v];

						if (item) {
							$("#"+chartConfig[v].graphcontainer + '-tooltip').remove();
							var x = item.datapoint[0],
							y = item.datapoint[1].toFixed(2);
							var content = "Time :  " + common.toServerLocalDateFromMilliSeconds(x);

							var dataset = cPlot.getData();
							var nDecimals = 2;
							if(chartConfig[v].ydecimals != null){
								nDecimals = chartConfig[v].ydecimals;
							}

							for (var i = 0; i < dataset.length; ++i) {
								var series = dataset[i];
								for (var j = 0; j < series.data.length; ++j) {
									if(series.data[j][0] == x){
										var text = chartConfig[v].ylabels[i] + " :  ";
										if(chartConfig[v].yvalformatter){
											text += chartConfig[v].yvalformatter(series.data[j][1].toFixed(nDecimals));
										}else{
											text += series.data[j][1].toFixed(nDecimals);
										}
										if(chartConfig[v].yunit){
											text += chartConfig[v].yunit;
										}
										content = content +  '<br/>' + text; 
									}
								}
							}
							common.showTooltip(pos.pageX, pos.pageY, content, chartConfig[v].graphcontainer + '-tooltip');
						} else {
							$("#"+chartConfig[v].graphcontainer + '-tooltip').remove();
						}

					});
				});

				$(DRILLDOWN_DIALOG).on('shown.bs.modal', function(event, ab){
					$(DRILLDOWN_CHART).empty();
					var metricName = $(DRILLDOWN_METRICNAME).text();
					if(metricName != "nodestatus"){
						$(DRILLDOWN_SPINNER).show();
						dashboardHandler.fetchMetricDrilldown(_this.generateParams(metricName, true));
					}else{
						$(DRILLDOWN_SERIES_CONTAINER).hide();
					}
				});
				$(SERIES_SELECTOR).on('change', function(){
					var metricName = $(DRILLDOWN_METRICNAME).text();
					dashboardHandler.fetchMetricDrilldown(_this.generateParams(metricName, true));
				});

				dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
				dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);
				dashboardHandler.on(dashboardHandler.DRILLDOWN_METRIC_SUCCESS, this.fetchDrilldownMetricSuccess); 
				dashboardHandler.on(dashboardHandler.DRILLDOWN_METRIC_ERROR, this.fetchDrilldownMetricError);
			}else{
				$('.dbmgr-ent').hide();
			}

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				if(lastUsedTimeRange == null){
					lastUsedTimeRange = {};
					var startTime = $(START_TIME_PICKER).data("DateTimePicker").date();
					var endTime = $(END_TIME_PICKER).data("DateTimePicker").date();

					lastUsedTimeRange.startTime = startTime.format('YYYY/MM/DD-HH:mm:ss');
					lastUsedTimeRange.endTime = endTime.format('YYYY/MM/DD-HH:mm:ss');
				}
			});

			$(FILTER_DIALOG).on('hide.bs.modal', function (e, v) {
				if(document.activeElement != $(FILTER_APPLY_BUTTON)[0]){
					_this.resetFilter(); //cancel clicked
				}
			});	
			this.refreshPage();
		},
		doResume: function(){
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$('#nodes-error-text').hide();
			$(window).on('resize', this.onResize);
			refreshTimer.resume();
			timeRangeView.resume();

			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.on(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);	
			timeRangeView.eventAgg.on(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);

			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 

			if(common.isEnterprise()){
				$('.dbmgr-ent').show();
				$(CANARY_DRILLDOWN_BTN).on('click', this.canaryDrillDown);
				$(TRANSACTIONS_DRILLDOWN_BTN).on('click', this.transactionsDrillDown);
				$(IOWAITS_DRILLDOWN_BTN).on('click',this.iowaitsDrillDown);
				$(DISK_SPACE_DRILLDOWN_BTN).on('click',this.diskspaceDrillDown);
				$(JVMGC_DRILLDOWN_BTN).on('click',this.jvmGCDrillDown);
				$(RSERVER_MEMORY_DRILLDOWN_BTN).on('click',this.rserverMemoryDrillDown);
				$(MEMSTORE_DRILLDOWN_BTN).on('click',this.memStoreDrillDown);
				$(CPULOAD_DRILLDOWN_BTN).on('click',this.cpuLoadDrillDown);
				$(FREEMEM_DRILLDOWN_BTN).on('click',this.freeMemoryDrillDown);
				$(NETWORKIO_DRILLDOWN_BTN).on('click',this.networkIODrillDown);
				$(NODES_DRILLDOWN_BTN).on('click',this.nodeStatusDrillDown);

				dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
				dashboardHandler.on(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);
				dashboardHandler.on(dashboardHandler.DRILLDOWN_METRIC_SUCCESS, this.fetchDrilldownMetricSuccess); 
				dashboardHandler.on(dashboardHandler.DRILLDOWN_METRIC_ERROR, this.fetchDrilldownMetricError);
			}else{
				$('.dbmgr-ent').hide();
			}
			this.refreshPage();
		},
		doPause: function(){
			$(window).off('resize', this.onResize);
			refreshTimer.pause();
			timeRangeView.pause();
			$(REFRESH_ACTION).off('click', this.refreshPage);
			refreshTimer.eventAgg.off(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.eventAgg.off(refreshTimer.events.INTERVAL_CHANGED, this.refreshIntervalChanged);	
			timeRangeView.eventAgg.off(timeRangeView.events.TIME_RANGE_CHANGED, this.timeRangeChanged);

			serverHandler.off(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.off(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.off(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.off(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 

			if(common.isEnterprise()){
				$(CANARY_DRILLDOWN_BTN).off('click', this.canaryDrillDown);
				$(TRANSACTIONS_DRILLDOWN_BTN).off('click', this.transactionsDrillDown);
				$(IOWAITS_DRILLDOWN_BTN).off('click',this.iowaitsDrillDown);
				$(DISK_SPACE_DRILLDOWN_BTN).off('click',this.diskspaceDrillDown);
				$(JVMGC_DRILLDOWN_BTN).off('click',this.jvmGCDrillDown);
				$(RSERVER_MEMORY_DRILLDOWN_BTN).off('click',this.rserverMemoryDrillDown);
				$(MEMSTORE_DRILLDOWN_BTN).off('click',this.memStoreDrillDown);
				$(CPULOAD_DRILLDOWN_BTN).off('click',this.cpuLoadDrillDown);
				$(FREEMEM_DRILLDOWN_BTN).off('click',this.freeMemoryDrillDown);
				$(NETWORKIO_DRILLDOWN_BTN).off('click',this.networkIODrillDown);
				$(NODES_DRILLDOWN_BTN).off('click',this.nodeStatusDrillDown);

				dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_SUCCESS, this.fetchSummaryMetricSuccess); 
				dashboardHandler.off(dashboardHandler.SUMMARY_METRIC_ERROR, this.fetchSummaryMetricError);
				dashboardHandler.off(dashboardHandler.DRILLDOWN_METRIC_SUCCESS, this.fetchDrilldownMetricSuccess); 
				dashboardHandler.off(dashboardHandler.DRILLDOWN_METRIC_ERROR, this.fetchDrilldownMetricError);
			}

		},
		onResize: function () {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doResize, 200);
		},
		doResize: function () {
			if(renderedFlotCharts !== null){
				$.each(renderedFlotCharts, function(index, graph){
					if (graph != null){
						var placeholder = graph.getPlaceholder();
						// somebody might have hidden us and we can't plot
						// when we don't have the dimensions
						if (placeholder.width() == 0 || placeholder.height() == 0)
							return;						
						graph.resize();
						graph.setupGrid();						
						graph.draw(); 
					}
				});
			}
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
		generateParams: function(metricName, isDrilldown){
			var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
			var endTime = $('#enddatetimepicker').data("DateTimePicker").date();

			var params = {};
			params.startTime = startTime.format('YYYY/MM/DD-HH:mm:ss');
			params.endTime = endTime.format('YYYY/MM/DD-HH:mm:ss');

			if(lastUsedTimeRange == null){
				lastUsedTimeRange = {};
			}		
			lastUsedTimeRange.startMsec = startTime.unix() * 1000;		
			lastUsedTimeRange.endMsec = endTime.unix() * 1000;
			lastUsedTimeRange.startTime = params.startTime;
			lastUsedTimeRange.endTime = params.endTime;
			lastUsedTimeRange.timeRange = $(FILTER_TIME_RANGE).val();

			params.metricName = metricName;
			params.isDrilldown = isDrilldown ? isDrilldown : false;
			params.timeinterval = endTime - startTime;
			timeinterval = params.timeinterval;
			if($(SERIES_SELECTOR)){
				params.seriesName = $(SERIES_SELECTOR+' option:selected').text();
			}
			return params;
		},
		resetFilter: function(){
			if(lastUsedTimeRange != null){
				$(FILTER_TIME_RANGE).val(lastUsedTimeRange.timeRange);
				//if(lastUsedTimeRange.timeRange == '0'){
				$(START_TIME_PICKER).data("DateTimePicker").date(moment(lastUsedTimeRange.startTime));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment(lastUsedTimeRange.endTime));
				//}
			}
		},
		refreshPage: function() {
			//If filter dialog is open, no refresh.
			if(($("#filterDialog").data('bs.modal') || {}).isShown == true){
				return;
			}

			timeRangeView.updateFilter();
			_this.fetchServices();
			_this.fetchNodes();

			if(common.isEnterprise()){
				$.each(Object.getOwnPropertyNames(chartConfig), function(k, v){
					$(chartConfig[v].spinner).show();
					dashboardHandler.fetchSummaryMetric(_this.generateParams(v));
				});
			}
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

				servicesTable = $('#services-results').DataTable({
					dom: 'tB',
					processing: true,
					"autoWidth": true,
					"scrollCollapse": true,
					"ordering": false,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ 
					                 { "aTargets": [ 0 ],
					                	 "mRender": function ( data, type, full ) {
					                		 if (type === 'display') {
					                			 if(data == 'DTM'){
					                				 return 'Transaction Manager';
					                			 }
					                			 if(data == 'RMS'){
					                				 return 'Runtime Manageability Service';
					                			 }
					                			 if(data == 'DCSMASTER'){
					                				 return 'DCS Master';
					                			 }
					                			 if(data == 'MXOSRVR'){
					                				 return 'Master Executor';
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
					                			 'Configured : '+full[1] + '<br/>Actual : '+full[2]+'<br/>Down : '+(full[3]&&full[3].length>0?full[3]:"0") +'"><i class="fa fa-check" ></i></button>';
					                		 }
					                		 else return data;
					                	 }
					                 }],
					                 "order": [],
					                 buttons: [
					                           { extend : 'copy', exportOptions: { columns: [0, 1, 2, 3] } },
					                           { extend : 'csv', exportOptions: { columns: [0, 1, 2, 3] } },
					                           { extend : 'excel', exportOptions: {  columns: [0, 1, 2, 3] } },
					                           { extend : 'pdfHtml5', exportOptions: { columns: [0, 1, 2, 3] }, title: 'Service Status' },
					                           { extend : 'print', exportOptions: { columns: [0, 1, 2, 3] }, title: 'Service Status' }
					                           ],					                 
					                           paging: true,
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

				var aoColumns = [];
				var aaData = [];

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var obj = new Object();
				obj.title = "Status";
				aoColumns.push(obj);

				var aaData = [];
				$.each(result.resultArray, function(i, data){
					var status = data[1];
					data.push(status);
					aaData.push(data);
				});

				_this.nodeStatusData = { aoColumns: aoColumns, aaData: aaData};

				_this.populateNodeStatus('nodes-results', NODES_RESULT_CONTAINER, false);
			}

		},
		populateNodeStatus: function(containerID, parent, isDrilldown){
			if(_this.nodeStatusData && _this.nodeStatusData.aaData){
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table dt-responsive" style="width:100%;" id="'+containerID +'"></table>';
				$(parent).html( sb );

				var bPaging = _this.nodeStatusData.aaData.length > 10;

				nodesTable = $('#'+containerID).DataTable({
					dom: isDrilldown ? '<"top"l<"clear">Bf>t<"bottom"rip>' : 'tif',
							paging: bPaging,
							"iDisplayLength" : 10, 
							"sPaginationType": "simple_numbers",
							"scrollCollapse": true,
							"aaData": _this.nodeStatusData.aaData, 
							"aoColumns" : _this.nodeStatusData.aoColumns,
							"aoColumnDefs": [
							                 {"aTargets": [0], "sWidth": "100px"},
							                 {"aTargets": [1], "sClass":"never", "bVisible":false},
							                 {
							                	 "aTargets": [ 2 ],
							                	 "mData": 2,
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
							                 buttons: [
							                           { extend : 'copy', exportOptions: { columns: [0, 1] } },
							                           { extend : 'csv', exportOptions: { columns: [0, 1] } },
							                           { extend : 'excel', exportOptions: { columns: [0, 1] } },
							                           { extend : 'pdfHtml5', exportOptions: { columns: [0, 1] }, title: 'Node Status' },
							                           { extend : 'print', exportOptions: { columns: [0, 1] }, title: 'Node Status' }
							                           ],					                 
							                           fnDrawCallback: function(){
							                        	   //$('#query-results td').css("white-space","nowrap");
							                           }

				});
				//nodesTable.buttons().container().appendTo($('#nodes-export-buttons') );
				$('#'+containerID+' td').css("white-space","nowrap");			
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
			metricConfig.toolTipTexts = {};

			if(keys.length == 0){
				$('#'+metricConfig.graphcontainer).hide();
				$(metricConfig.errorcontainer).text("No data available");
				$(metricConfig.errorcontainer).show();				
			}else{
				$('#'+metricConfig.graphcontainer).show();
				$(metricConfig.errorcontainer).hide();

				var plotData = [];
				$.each(result.data[keys[0]], function(i, v){
					plotData.push([]);
				});

				$.each(keys, function(index, value){
					var xVal = metricConfig.xtimemultiplier? value*metricConfig.xtimemultiplier: value;
					//var dataPoint = [];
					$.each(result.data[value], function(i, v){
						var yVal = 0;
						if(metricConfig.deltamultiplier){
							yVal = v * metricConfig.deltamultiplier;	
						}else{
							yVal = v;
						}
						if(yVal < 0){
							yVal = 0;
						}
						if(metricConfig.yvalround == true){
							yVal = Math.round(yVal);
						}
						//dataPoint.push(yVal);
						plotData[i].push([xVal, yVal]);
					});
					//metricConfig.toolTipTexts[xVal] = dataPoint;
				});

				var flotOptions = {
						colors : graphColors,
						canvas: true,
						xaxis : {
							min: lastUsedTimeRange.startMsec,
							max: lastUsedTimeRange.endMsec,
							mode : "time", 
							tickFormatter: function(val, axis) {
								return common.formatGraphDateLabels(val, timeinterval);
							},
						},
						yaxis :{
							min: 0,
							show:true,
							tickFormatter: function(val, axis){
								if(metricConfig.yLabelFormat){
									return metricConfig.yLabelFormat(val);
								}
								return val.toFixed(0);
							}
						},
						series : {
							shadowSize: 0,
							lines:{
								show:true
							}
						},
						/*crosshair: {
							mode: "x"
						},*/
						lines: {
							lineWidth: 2.5,
							fill: false,
						},
						grid : {
							hoverable: true,
							borderColor: "#f3f3f3",
							borderWidth: {top:0, right: 0, bottom: 1, left: 1},
							tickColor: "#737373"
						},
						yaxes:[
						       {
						    	   tickLength:0,
						    	   font: {
						    		   size: 12,
						    		   lineHeight: 11,
						    		   family: "sans-serif",
						    		   variant: "small-caps",
						    		   color: 'black'
						    	   }
						       }],
						       xaxes:[{
						    	   tickLength : 5,
						    	   font: {
						    		   size: 12,
						    		   lineHeight: 11,
						    		   family: "sans-serif",
						    		   variant: "small-caps",
						    		   color: 'black'
						    	   }
						       }]
				};

				renderedFlotCharts[result.metricName] = $.plot($('#'+metricConfig.graphcontainer), plotData, flotOptions);
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
		canaryDrillDown: function(){
			_this.displayDetails('canary');
		},
		transactionsDrillDown: function(){
			_this.displayDetails('transactions');
		},
		iowaitsDrillDown: function(){
			_this.displayDetails('iowaits');
		},
		diskspaceDrillDown: function(){
			_this.displayDetails('useddiskspace');
		},
		jvmGCDrillDown: function(){
			_this.displayDetails('jvmgctime');
		},
		rserverMemoryDrillDown: function(){
			_this.displayDetails('regionservermemory');
		},
		memStoreDrillDown: function(){
			_this.displayDetails('memstoresize');
		},
		cpuLoadDrillDown: function(){
			_this.displayDetails('cpuload');
		},
		freeMemoryDrillDown: function(){
			_this.displayDetails('freememory');
		},
		networkIODrillDown: function(){
			_this.displayDetails('networkio');
		},
		nodeStatusDrillDown: function(){
			$(DRILLDOWN_DIALOG).modal('show');
			$(DRILLDOWN_TITLE).text("Node Status");
			$(DRILLDOWN_METRICNAME).text("nodestatus");
			$(DRILLDOWN_SERIES_CONTAINER).hide();
			$(DRILLDOWN_SPINNER).hide();
			$(DRILLDOWN_CHART_CONTAINER).hide();
			$(GRID_DRILLDOWN_CONTAINER).show();
			_this.populateNodeStatus('nodes-results-drilldown', GRID_DRILLDOWN_CONTAINER, true);
		},
		displayDetails: function(metricName){
			$(DRILLDOWN_DIALOG).modal('show');
			$(DRILLDOWN_SERIES_CONTAINER).show();
			$(DRILLDOWN_METRICNAME).text(metricName);
			$(DRILLDOWN_TITLE).text(chartConfig[metricName]);
			var seriesSelector = $(SERIES_SELECTOR);
			seriesSelector.empty();
			var metricConfig = chartConfig[metricName];
			$.each(metricConfig.ylabels, function(i, v){
				seriesSelector.append($("<option></option>").val(i).html(v));
			});
			$(GRID_DRILLDOWN_CONTAINER).hide();
		},
		fetchDrilldownMetricSuccess:function(result){
			var metricsData = JSON.parse(result.data.metrics);
			var metricConfig = chartConfig[result.metricName];
			var tags = result.data.tags;
			var keys = Object.keys(metricsData);
			drillDownChart= {};
			drillDownChart.metricConfig = metricConfig;

			$(DRILLDOWN_TITLE).text(metricConfig.chartTitle);
			$(DRILLDOWN_SPINNER).hide();

			if(keys.length == 0){
				$(DRILLDOWN_CHART_CONTAINER).hide();
				$(DRILLDOWN_ERROR_CONTAINER).text("No data available");
				$(DRILLDOWN_ERROR_CONTAINER).show();				
			}else{
				$(DRILLDOWN_ERROR_CONTAINER).text("");
				$(DRILLDOWN_ERROR_CONTAINER).hide();
				$(DRILLDOWN_CHART).empty();
				$(DRILLDOWN_CHART_CONTAINER).show();
				$(DRILLDOWN_LEGEND).empty();

				drillDownChart.plotData = [];
				$.each(metricsData	[keys[0]], function(i, v){
					var seriesData = {label: "", data:[], color: ""};
					seriesData.label = result.data.tags[i];
					seriesData.labelID = "lbl-" + i;
					seriesData.color = graphColors[i];
					drillDownChart.plotData.push(seriesData);
				});

				$.each(keys, function(index, value){
					var xVal = metricConfig.xtimemultiplier? value*metricConfig.xtimemultiplier: value;
					//var dataPoint = [];
					$.each(metricsData[value], function(i, v){
						var yVal = 0;
						if(metricConfig.deltamultiplier){
							yVal = v * metricConfig.deltamultiplier;	
						}else{
							yVal = v;
						}
						if(yVal < 0){
							yVal = 0;
						}
						if(metricConfig.yvalround == true){
							yVal = Math.round(yVal);
						}
						//dataPoint.push(yVal);
						drillDownChart.plotData[i].data.push([xVal, yVal]);
					});
					//metricConfig.toolTipTexts[xVal] = dataPoint;
				});

				$(DRILLDOWN_LEGEND).append("<br/><label id='x-time'></label>");
				$.each(result.data.tags, function(key, val) {
					$(DRILLDOWN_LEGEND).append("<br/><input type='checkbox' name='" + key +
							"' checked='checked' id='id" + key + "'><span class='drilldown-legend-selector' style='background-color:"+  graphColors[key] + ";'></span></input>" +
							"<label for='id" + key + "'>"
							+ val + "</label> <label id='lbl-" + key + "' class='y-val-label'></label>");
				});

				$(DRILLDOWN_LEGEND).find("input").click(_this.plotAccordingToChoices);

				drillDownChart.flotOptions = {
						//colors : graphColors,
						canvas: true,
						axisLabels: {
							show: true
						},
						legend: {
							show: false,
							/*noColumns: 1,
							container: $(DRILLDOWN_LEGEND),*/
						},
						crosshair: {
							mode: "x"
						},
						xaxis : {
							mode : "time", 
							min: lastUsedTimeRange.startMsec,
							max: lastUsedTimeRange.endMsec,
							tickFormatter: function(val, axis) {
								return common.formatGraphDateLabels(val, timeinterval);
							},
						},
						yaxis :{
							min: 0,
							show:true,
							tickFormatter: function(val, axis){
								if(metricConfig.yLabelFormat){
									return metricConfig.yLabelFormat(val);
								}
								return val.toFixed(0);
							}
						},
						series : {
							shadowSize: 0,
							lines:{
								show:true
							}
						},	
						lines: {
							lineWidth: 2.5,
							fill: false
						},
						grid : {
							hoverable: true,
							borderColor: "#f3f3f3",
							borderWidth: {top:0, right: 0, bottom: 1, left: 1},
							tickColor: "#737373",
							autoHighlight: false
						},
						yaxes:[{
							position: 'left',
							axisLabel: (metricConfig.yunit ? metricConfig.yunit : ""),
							axisLabelPadding: 10,
							axisLabelColour: 'red',
							axisLabelFontSizePixels: 13,
							tickLength:5,
							font: {
								size: 12,
								lineHeight: 11,
								family: "sans-serif",
								variant: "small-caps",
								color: 'black'
							}
						}],
						xaxes:[{
							axisLabel: 'Time',
							axisLabelPadding: 10,
							axisLabelColour: 'red',
							axisLabelFontSizePixels: 13,
							tickLength : 5,
							font: {
								size: 12,
								lineHeight: 11,
								family: "sans-serif",
								variant: "small-caps",
								color: 'black'
							}
						}]
				};
				_this.plotAccordingToChoices();

				drillDownChart.updateLegendTimeout = null;
				drillDownChart.latestPosition = null;



				$(DRILLDOWN_CHART).bind("plothover",  function (event, pos, item) {
					drillDownChart.latestPosition = pos;
					if (!drillDownChart.updateLegendTimeout) {
						drillDownChart.updateLegendTimeout = setTimeout(_this.updateLegend, 50);
					}
				});				
			}			
		},
		plotAccordingToChoices: function(e) {

			var data = [];

			$(DRILLDOWN_LEGEND).find('#x-time').text('');
			$(DRILLDOWN_LEGEND).find(".y-val-label").text('');

			$(DRILLDOWN_LEGEND).find("input:checked").each(function () {
				var key = $(this).attr("name");
				if (key && drillDownChart.plotData[key]) {
					data.push(drillDownChart.plotData[key]);
				}
			});

			if (data.length > 0) {
				drillDownChart.plot = $.plot($(DRILLDOWN_CHART), data, drillDownChart.flotOptions);
			}else{
				if(e != null) // At least one series should be selected. Cancel the click event.
					e.preventDefault();
			}
		},
		updateLegend: function() {

			drillDownChart.updateLegendTimeout = null;

			var pos = drillDownChart.latestPosition;

			var axes = drillDownChart.plot.getAxes();
			if (pos.x < axes.xaxis.min || pos.x > axes.xaxis.max ||
					pos.y < axes.yaxis.min || pos.y > axes.yaxis.max) {
				return;
			}

			var i, j, dataset = drillDownChart.plot.getData();
			for (i = 0; i < dataset.length; ++i) {

				var series = dataset[i];

				// Find the nearest points, x-wise

				for (j = 0; j < series.data.length; ++j) {
					if (series.data[j][0] > pos.x) {
						break;
					}
				}

				// Now Interpolate

				var y, x,
				p1 = series.data[j - 1],
				p2 = series.data[j];

				if (p1 == null) {
					y = p2[1];
					x = p2[0];
				} else if (p2 == null) {
					y = p1[1];
					x = p1[0];
				} else {
					y = p1[1] + (p2[1] - p1[1]) * (pos.x - p1[0]) / (p2[0] - p1[0]);
					x = p1[0] + (p2[0] - p1[0]) * (pos.x - p1[0]) / (p2[0] - p1[0]);
				}

				var nDecimals = 2;
				if(drillDownChart.metricConfig.ydecimals != null){
					nDecimals = drillDownChart.metricConfig.ydecimals;
				}

				var text = " =  ";
				if(drillDownChart.metricConfig.yvalformatter){
					text += drillDownChart.metricConfig.yvalformatter(y.toFixed(nDecimals));
				}else{
					text += y.toFixed(nDecimals);
				}
				if(drillDownChart.metricConfig.yunit){
					text += drillDownChart.metricConfig.yunit;
				}
				$(DRILLDOWN_LEGEND).find('#x-time').text("Time :  " + common.toServerLocalDateFromMilliSeconds(x));
				$(DRILLDOWN_LEGEND).find('#'+series.labelID).text(text);
			}
		},
		fetchDrilldownMetricError:function(jqXHR, res, error){
			var metricConfig = chartConfig[jqXHR.metricName];
			$(DRILLDOWN_SPINNER).hide();
			$(DRILLDOWN_CHART_CONTAINER).hide();

			if (jqXHR.responseText) {
				$(DRILLDOWN_ERROR_CONTAINER).text(jqXHR.responseText);     
				$(DRILLDOWN_ERROR_CONTAINER).show();
			}				
		}
	});


	return DashboardView;
});
