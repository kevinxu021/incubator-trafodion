define([
    'views/BaseView',
    'raphael',
    'morris',
    'text!templates/dashboard.html'
], function (BaseView, Raphael, Morris, DashboardT) {
    'use strict';

    var _that = null;
    var cpuGraph = null, diskReadsGraph = null, diskWritesGraph=null, getOpsGraph = null;
    
    var DashboardView = BaseView.extend({
    	template:  _.template(DashboardT),

		init: function (){
			_that = this;
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();
			
			$("#refreshAction").on('click', this.fetchCPUData);
		},
		resume: function(){
			this.fetchDiskReads();
			this.fetchDiskWrites();
			this.fetchGetOps();
			$("#refreshAction").on('click', this.fetchCPUData);
		},
		pause: function(){
			$("#refreshAction").off('click', this.fetchCPUData);
		},
        showLoading: function(){
        	$('#cpuLoadingImg').show();
        },

        hideLoading: function () {
        	$('#cpuLoadingImg').hide();
        },
		fetchCPUData: function () {
			_that.showLoading();

			$.ajax({
				url:'resources/metrics/cpu',
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displayCPUGraph,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
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
			$.ajax({
				url:'resources/metrics/diskreads',
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displayDiskReadsGraph,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},

		displayDiskReadsGraph: function (result){
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
		fetchDiskWrites: function () {
			$('#writesLoadingImg').hide();
			$.ajax({
				url:'resources/metrics/diskwrites',
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displayDiskWritesGraph,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},

		displayDiskWritesGraph: function (result){
			//_that.hideLoading();
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
		fetchGetOps: function () {
			$('#thruputLoadingImg').hide();
			$.ajax({
				url:'resources/metrics/getops',
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displayGetOpsGraph,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},

		displayGetOpsGraph: function (result){
			//_that.hideLoading();
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
