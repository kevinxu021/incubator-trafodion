define([
    'views/BaseView',
    'raphael',
    'morris',
    'text!templates/dashboard.html',
    'handlers/DashboardHandler'
], function (BaseView, Raphael, Morris, DashboardT, dashboardHandler) {
    'use strict';

    var _that = null;
    var cpuGraph = null, diskReadsGraph = null, diskWritesGraph=null, getOpsGraph = null;
    
    var DashboardView = BaseView.extend({
    	template:  _.template(DashboardT),

		init: function (){
			_that = this;
			
			$("#refreshAction").on('click', this.refreshGraphs);
			dashboardHandler.on(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.on(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.on(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.on(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.on(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.on(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); 
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();
		},
		resume: function(){
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();
			$("#refreshAction").on('click', this.refreshGraphs);
			dashboardHandler.on(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.on(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.on(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.on(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.on(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.on(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); 
		},
		pause: function(){
			dashboardHandler.off(dashboardHandler.DISKREADS_SUCCESS, this.fetchDiskReadsSuccess);
			dashboardHandler.off(dashboardHandler.DISKREADS_ERROR, this.fetchDiskReadsError);        	  
			dashboardHandler.off(dashboardHandler.DISKWRITES_SUCCESS, this.fetchDiskWritesSuccess);
			dashboardHandler.off(dashboardHandler.DISKWRITES_ERROR, this.fetchDiskWritesError);        	  
			dashboardHandler.off(dashboardHandler.GETOPS_SUCCESS, this.fetchGetOpsSuccess);
			dashboardHandler.off(dashboardHandler.GETOPS_ERROR, this.fetchGetOpsError); 
			$("#refreshAction").off('click', this.refreshGraphs);
		},
        showLoading: function(){
        	$('#cpuLoadingImg').show();
        },

        hideLoading: function () {
        	$('#cpuLoadingImg').hide();
        },
        refreshGraphs: function() {
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();        	
        },
		fetchCPUData: function () {
			_that.showLoading();
			dashboardHandler.fetchCPUData();
		},

		displayCPUGraph: function (result){
			_that.hideLoading();
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
						  return "Disk Reads : " + row.y + "<br/>Time : " + row.x;
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
        	_that.hideLoading();
        	$("#errorText").show();
        	if (jqXHR.responseText) {
        		$("#errorText").text(jqXHR.responseText);
        	}
        }  
    });
    

    return DashboardView;
});
