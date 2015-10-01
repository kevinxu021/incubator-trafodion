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
    'datatables',
    'datatablesBootStrap',
    'tabletools'
], function (BaseView, Raphael, Morris, DashboardT, dashboardHandler, serverHandler, $, common, moment) {
    'use strict';

    var _this = null;
    var SERVICES_SPINNER = '#services-spinner',
	    SERVICES_RESULT_CONTAINER = '#services-result-container',
	    SERVICES_ERROR_TEXT = '#services-error-text',
	    NODES_SPINNER = '#nodes-spinner',
	    NODES_RESULT_CONTAINER = '#nodes-result-container',
	    NODES_ERROR_TEXT = '#nodes-error-text',
	    REFRESH_ACTION = '#refreshAction',
	    REFRESH_INTERVAL = '#refreshInterval',
	    TIME_RANGE = '#timeRange';
    
    var OPEN_FILTER = '#openFilter',
    	FILTER_DIALOG = '#filterDialog',
    	FILTER_FORM = '#filter-form',
		FILTER_APPLY_BUTTON = "#filterApplyButton",
		FILTER_START_TIME = '#filter-start-time',
		FILTER_END_TIME = '#filter-end-time';
    
    var validator = null;
	
    var cpuGraph = null, diskReadsGraph = null, diskWritesGraph=null, getOpsGraph = null, canaryGraph = null;
    var servicesTable = null, nodesTable = null;
    
    var DashboardView = BaseView.extend({
    	template:  _.template(DashboardT),
    	
		init: function (){
			
			_this = this;
			
			validator = $(FILTER_FORM).validate({
				rules: {
					"filter-start-time": { required: true },
					"filter-end-time": { required: true }
				},
				highlight: function(element) {
			        $(element).closest('.form-group').addClass('has-error');
			    },
			    unhighlight: function(element) {
			        $(element).closest('.form-group').removeClass('has-error');
			    },
			    errorElement: 'span',
			    errorClass: 'help-block',
		        errorPlacement: function(error, element) {
		            if(element.parent('.input-group').length) {
		                error.insertAfter(element.parent());
		            } else {
		                error.insertAfter(element);
		            }
		        }
			});
			$(TIME_RANGE).val(-1);
			
			$(FILTER_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(FILTER_APPLY_BUTTON).attr('disabled', false);
				} else {
					$(FILTER_APPLY_BUTTON).attr('disabled', true);
				}
			});
			
			$('#startdatetimepicker').datetimepicker({format: 'YYYY-MM-DD HH:mm:ss z'});
			$('#enddatetimepicker').datetimepicker({format: 'YYYY-MM-DD HH:mm:ss z'});
			$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				$('#filter-start-time').prop("disabled", false);
				$('#filter-end-time').prop("disabled", false);
			});

			$(TIME_RANGE).focusin(function(){
				$(this).val(-1);
			});
		
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$(NODES_ERROR_TEXT).hide();
			
			/*dashboardHandler.on(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.on(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.on(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.on(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.on(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.on(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); 
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();*/
			
			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.on(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.on(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(TIME_RANGE).on('change', this.timeRangeChanged);
			$(REFRESH_INTERVAL).on('change', this.refreshIntervalChanged);

			$(REFRESH_INTERVAL).val('1').trigger('change');
			$(TIME_RANGE).val(1).trigger('change');
		},
		resume: function(){
			$(REFRESH_ACTION).on('click', this.refreshPage);
			$(SERVICES_ERROR_TEXT).hide();
			$('#nodes-error-text').hide();
			/*dashboardHandler.on(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.on(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.on(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.on(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.on(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.on(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); 
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();*/
			serverHandler.on(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.on(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.on(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.on(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.on(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.on(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(TIME_RANGE).on('change', this.timeRangeChanged);
			$(REFRESH_INTERVAL).on('change', this.refreshIntervalChanged);
			$(REFRESH_ACTION).on('click', this.refreshPage);
			
			this.refreshPage();
		},
		pause: function(){

			/*dashboardHandler.off(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.off(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.off(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.off(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.off(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.off(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); */
			serverHandler.off(serverHandler.FETCH_SERVICES_SUCCESS, this.fetchServicesSuccess);
			serverHandler.off(serverHandler.FETCH_SERVICES_ERROR, this.fetchServicesError); 
			serverHandler.off(serverHandler.FETCH_NODES_SUCCESS, this.fetchNodesSuccess);
			serverHandler.off(serverHandler.FETCH_NODES_ERROR, this.fetchNodesError); 
			dashboardHandler.off(dashboardHandler.CANARY_SUCCESS, this.fetchCanarySuccess); 
			dashboardHandler.off(dashboardHandler.CANARY_ERROR, this.fetchCanaryError);
			
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(TIME_RANGE).off('change', this.timeRangeChanged);
			$(REFRESH_INTERVAL).off('change', this.refreshIntervalChanged);			
			$(REFRESH_ACTION).off('click', this.refreshPage);
		},
        showLoading: function(){
        	$('#cpuLoadingImg').show();
        },

        hideLoading: function () {
        	$('#cpuLoadingImg').hide();
        },

        updateTimeRangeLabel: function(){
			$('#timeRangeLabel').text('Time Range : ' +  $('#startdatetimepicker').data("DateTimePicker").date().format('YYYY-MM-DD HH:mm:ss') + ' - ' +  $('#enddatetimepicker').data("DateTimePicker").date().format('YYYY-MM-DD HH:mm:ss'));
        },
        openCustomTimeDialog: function(){
        	$(FILTER_DIALOG).modal('show');
        },
        filterApplyClicked: function(){
        	if(!$(FILTER_FORM).valid()){
        		return;
        	}
        	_this.updateTimeRangeLabel();
        	
        	var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
        	var endTime = $('#enddatetimepicker').data("DateTimePicker").date();
        	
        	var param = {};
        	param.startTime = startTime.format('YYYY-MM-DD HH:mm:ss');
        	param.endTime = endTime.format('YYYY-MM-DD HH:mm:ss');
        	
        	$(FILTER_DIALOG).modal('hide');
        	//_this.showLoading();
        	_this.refreshPage();
        },        
        timeRangeChanged: function(){
			var sel = $(this).val();
			$(this).blur();
			if(sel == '0'){
				_this.openCustomTimeDialog();
			}else{
				_this.refreshPage();
			}
        },
        updateFilter: function() {
        	var sel = $(TIME_RANGE).val();
        	switch(sel){
			case "1":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "6":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "24":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;
			case "128":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'week'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				break;			
			}
        	_this.updateTimeRangeLabel();
        },
        refreshIntervalChanged: function(){
        	var interval = $(REFRESH_INTERVAL).val();
    		_this.refreshTimer.set(interval);
    		_this.updateFilter();
        },
        onTimerBeeped: function(){
        	_this.refreshPage();
        },
        refreshPage: function() {
        	_this.updateFilter();
			/*this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();*/
        	_this.fetchServices();
        	_this.fetchNodes();
        	_this.fetchCanaryResponse();
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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="services-results"></table>';
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
					"aoColumnDefs": [ {
					      "aTargets": [ 4 ],
					      "mData": 4,
					      "sWidth":"50px",
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					    	   if(data == 'ERROR'){
					    		   return '<button type="button" class="btn btn-danger btn-circle btn-small"><i class="fa fa-times"></i></button>';
					    	   }
					    	   if(data == 'ERROR'){
					    		   return '<button type="button" class="btn btn-warning btn-circle btn-small"><i class="fa fa-warning"></i></button>';
					    	   }
				    		   return '<button type="button" class="btn btn-success btn-circle btn-small"><i class="fa fa-check"></i></button>';
					        }
					        else return data;
					      }
					    },
					    { "aTargets": [ 0 ], sTitle: "SERVICE", "mData": 0, "sWidth":"60px",
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
					    {"aTargets": [1,2,3], "sWidth": "50px"}
					    ],
					paging: true,
					/*"tableTools": {
						"sRowSelect": "none",
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},*/
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             }
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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="nodes-results"></table>';
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
			$('#canarySpinner').show();
			dashboardHandler.fetchCanaryResponse();       	
        },
        fetchCanarySuccess: function(result){
			$('#canarySpinner').hide();
			var keys = Object.keys(result);
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
					hideHover: 'auto',
					hoverCallback: function (index, options, content, row) {
						  return "Canary Response Time : " + row.y + " msec <br/>Time : " + common.toServerLocalDateFromUtcMilliSeconds(row.x);
						}
				});
			}else{
				canaryGraph.setData(seriesData);
			}			
		},
		fetchCanaryError: function(){
			$('#canarySpinner').hide();
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
						  return "Disk Writes : " + row.y + "<br/>Time : " + row.x;
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
						  return "Throughput : " + row.y + "<br/>Time : " + row.x;
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
