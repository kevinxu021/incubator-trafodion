//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workloads_history.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datetimepicker',
        'jqueryvalidate',
        'datatables.net-buttons',
        'datatables.net-select',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	RESULT_CONTAINER = '#repo-result-container',
	ERROR_CONTAINER = '#repo-error-text',
	REFRESH_MENU = '#refreshAction',
	QCANCEL_MENU = '#cancelAction';

	var DATE_FORMAT = 'YYYY-MM-DD HH:mm:ss',
	DATE_FORMAT_ZONE = DATE_FORMAT + ' z',
	TIME_RANGE_LABEL = '#timeRangeLabel',
	START_TIME_PICKER = '#startdatetimepicker',
	END_TIME_PICKER = '#enddatetimepicker';

	var OPEN_FILTER = '#openFilter',
	FILTER_DIALOG = '#filterDialog',
	FILTER_FORM = '#filter-form',
	FILTER_APPLY_BUTTON = "#filterApplyButton",
	FILTER_START_TIME = '#filter-start-time',
	FILTER_END_TIME = '#filter-end-time',
	FILTER_QUERY_IDS = '#filter-query-ids',
	FILTER_USER_NAMES = '#filter-user-names',
	FILTER_APP_NAMES = '#filter-app-names',
	FILTER_CLIENT_NAMES = '#filter-client-names',
	FILTER_QUERY_TEXT = '#filter-query-text',
	FILTER_TIME_RANGE = '#filter-time-range',
	FILTER_MAX_FETCH_ROWS = '#max-fetch-rows';
	
	var CANCEL_QUERY_DIALOG = '#cancel-query-dialog',
	CANCEL_QUERY_ID = '#cancel-query-id',
	CANCEL_QUERY_YES_BTN = '#cancel-query-yes-btn';

	var CHART_CONFIG = {
			"TopN_Memory_Used":{
				container:"topN-memory-chart",
				spinner:"#memory-spinner",
				error:"#topN-memory-error-text",
				value:"max_mem_used" //key to get value from return data
			},
			"TopN_CPU_Time":{
				container:"topN-cpu-chart",
				spinner:"#cpu-spinner",
				error:"#topN-cpu-error-text",
				value:"cpu_time"
			},
			"TopN_Total_Runtime":{
				container:"topN-runtime-chart",
				spinner:"#runtime-spinner",
				error:"#topN-runtime-error-text",
				value:"query_elapsed_time"
			},
			"TopN_Disk_IO":{
				container:"topN-diskio-chart",
				spinner:"#diskio-spinner",
				error:"#topN-diskio-error-text",
				value:"disk_ios"
			},
	}
	var FLOT_OPTIONS = {
			grid :{
				hoverable: true,
				borderColor: "#f3f3f3",
				borderWidth: {top:0, right: 0, bottom: 1, left: 1},
				tickColor: "#737373"
				},
			series: {
		        points: {
		            fillColor: 'red'
		        }
		    },
			xaxis : {
				mode : "time",
				tickLength:0
			},
			yaxis : {
				tickLength:0,
				min:1,
				max:5,
				tickFormatter: function (val,axis) { return (6-val); },
			}
	}
	
	var oDataTable = null;
	var _this = null;
	var validator = null;
	var resizeTimer = null;
	var lastAppliedFilters = null; //last set of filters applied by user explicitly
	var lastUsedTimeRange = null;
	
	var WorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			_this = this;

			$.validator.addMethod("validateStartAndEndTimes", function(value, element) {
				var startTime = new Date($(START_TIME_PICKER).data("DateTimePicker").date()).getTime();
				var endTime = new Date($(END_TIME_PICKER).data("DateTimePicker").date()).getTime();
				return (startTime > 0 && startTime < endTime);
			}, "* Invalid Date Time or Start Time is not less than End Time");

			validator = $(FILTER_FORM).validate({
				rules: {
					"filter-start-time": { required: true, validateStartAndEndTimes: true },
					"filter-end-time": { required: true, validateStartAndEndTimes: true }
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

			$(FILTER_FORM).bind('change', function() {
				if($(this).validate().checkForm()) {
					$(FILTER_APPLY_BUTTON).attr('disabled', false);
				} else {
					$(FILTER_APPLY_BUTTON).attr('disabled', true);
				}
			});

			$(START_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			$(END_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			/*$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));*/
			this.initialTimeRangePicker();

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				_this.updateFilter();
			});

			$(FILTER_DIALOG).on('hide.bs.modal', function (e, v) {
				if(document.activeElement != $(FILTER_APPLY_BUTTON)[0]){
					validator.resetForm();
					_this.resetFilter();  //cancel clicked
				}
			});

			$(FILTER_TIME_RANGE).change(function(){
				var sel = $(this).val();
				switch(sel){
				case "1":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$(FILTER_START_TIME).prop("disabled", true);
					$(FILTER_END_TIME).prop("disabled", true);
					break;
				case "6":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$(FILTER_START_TIME).prop("disabled", true);
					$(FILTER_END_TIME).prop("disabled", true);
					break;
				case "24":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$(FILTER_START_TIME).prop("disabled", true);
					$(FILTER_END_TIME).prop("disabled", true);
					break;
				case "128":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'week'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$(FILTER_START_TIME).prop("disabled", true);
					$(FILTER_END_TIME).prop("disabled", true);
					break;	
				case "0":
					$(FILTER_START_TIME).prop("disabled", false);
					$(FILTER_END_TIME).prop("disabled", false);
				}
			});

			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			wHandler.on(wHandler.FETCH_TOPN_MAX_MEM_USED_SUCCESS, this.displayTopMemUsed);
			wHandler.on(wHandler.FETCH_TOPN_CPU_TIME_SUCCESS, this.displayTopCPUTime);
			wHandler.on(wHandler.FETCH_TOPN_Rumtime_SUCCESS, this.displayTopRuntime);
			wHandler.on(wHandler.FETCH_TOPN_DiskIO_SUCCESS, this.displayTopDiskIO);
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			this.fetchQueriesInRepository();
			_this.fetchTopN();
		},
		doResume: function(){
			this.redirectFlag=false;
			this.initialTimeRangePicker();
			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			//this.fetchQueriesInRepository();
			if(lastUsedTimeRange != null){
				var currTimeRange = $(FILTER_TIME_RANGE).val();
				if(lastUsedTimeRange != currTimeRange){
					this.fetchQueriesInRepository();
				}
			}
		},
		doPause: function(){
			this.redirectFlag=true;
			this.storeCommonTimeRange();
			wHandler.off(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchQueriesInRepository);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).off('click', this.cancelQueryConfirmed);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
		},
		initialTimeRangePicker:function(){
			if(common.commonTimeRange==null){
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$(FILTER_TIME_RANGE).val("1");
			}else{
				if(common.commonTimeRange.timeRangeTag=="0"){
					$(START_TIME_PICKER).data("DateTimePicker").date(common.commonTimeRange.startTime);
					$(END_TIME_PICKER).data("DateTimePicker").date(common.commonTimeRange.endTime);
				}else{
					_this.updateFilter(common.commonTimeRange.timeRangeTag);
				}
				_this.updateTimeRangeLabel();
				$(FILTER_TIME_RANGE).val(common.commonTimeRange.timeRangeTag);
			}
			lastAppliedFilters =  _this.getFilterParams();
		},
		storeCommonTimeRange:function(){
			var selection = $(FILTER_TIME_RANGE).val();
			common.getCommonTimeRange(selection);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},
		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		updateFilter: function(selection){
			if(selection==null){
				selection = $(FILTER_TIME_RANGE).val();
			}else{
				$(FILTER_TIME_RANGE).val(selection);
			}
			switch(selection){
			case "1":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$(FILTER_START_TIME).prop("disabled", true);
				$(FILTER_END_TIME).prop("disabled", true);
				break;
			case "6":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$(FILTER_START_TIME).prop("disabled", true);
				$(FILTER_END_TIME).prop("disabled", true);
				break;
			case "24":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$(FILTER_START_TIME).prop("disabled", true);
				$(FILTER_END_TIME).prop("disabled", true);
				break;
			case "128":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'week'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$(FILTER_START_TIME).prop("disabled", true);
				$(FILTER_END_TIME).prop("disabled", true);
				break;					
			case "0":
				$(FILTER_START_TIME).prop("disabled", false);
				$(FILTER_END_TIME).prop("disabled", false);
			}  
			_this.updateTimeRangeLabel();
		},
		updateTimeRangeLabel: function(){
			$(TIME_RANGE_LABEL).text('Time Range : ' +  $(START_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE) + ' - ' +  $(END_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE));
		},
		filterButtonClicked: function(){
			$(FILTER_DIALOG).modal('show');
		},
		resetFilter:function(){
			$(FILTER_MAX_FETCH_ROWS).val('5000');
			$(FILTER_QUERY_IDS).val('');
			$(FILTER_USER_NAMES).val('');
			$(FILTER_APP_NAMES).val('');
			$(FILTER_CLIENT_NAMES).val('');
			$(FILTER_QUERY_TEXT).val('');  

			$('#state-completed').prop('checked',false);
			$('#state-executing').prop('checked', false);
			$('#state-init').prop('checked', false)
			$('#state-unknown').prop('checked', false)

			if(lastAppliedFilters != null){
				$(FILTER_TIME_RANGE).val(lastAppliedFilters.timeRange);
				if(lastAppliedFilters.timeRange == '0'){
					$(START_TIME_PICKER).data("DateTimePicker").date(moment(lastAppliedFilters.startTime));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment(lastAppliedFilters.endTime));
				}

				$(FILTER_QUERY_IDS).val(lastAppliedFilters.queryIDs);
				$(FILTER_USER_NAMES).val(lastAppliedFilters.userNames);
				$(FILTER_APP_NAMES).val(lastAppliedFilters.appNames);
				$(FILTER_CLIENT_NAMES).val(lastAppliedFilters.clientNames);
				$(FILTER_QUERY_TEXT).val(lastAppliedFilters.queryText);  

				var states = lastAppliedFilters.states.split(',');
				$.each(states, function(index, value){
					if($('#state-' + value.toLowerCase()))
						$('#state-' + value.toLowerCase()).prop('checked', true);
				});
			}
		},
		filterApplyClicked: function(source){
			if($(FILTER_FORM).valid()){

			}else{
				_this.hideLoading();
				return;
			}
			_this.updateTimeRangeLabel();

			var param = _this.getFilterParams();
			
			lastUsedTimeRange = $(FILTER_TIME_RANGE).val();
				
			if(lastAppliedFilters == null || source != null){
				lastAppliedFilters = param;
			}else{
				/*if(param.timeRange != '0'){
					lastAppliedFilters.startTime = param.startTime;
					lastAppliedFilters.endTime = param.endTime;
				}*/
			}

			if(source != null && $(source.currentTarget)[0] == $(FILTER_APPLY_BUTTON)[0]){
				$(FILTER_DIALOG).modal('hide');
			}
			_this.showLoading();
			wHandler.fetchQueriesInRepository(lastAppliedFilters);
			_this.fetchTopN();
		},
		getFilterParams: function(){
			var startTime = $(START_TIME_PICKER).data("DateTimePicker").date();
			var endTime = $(END_TIME_PICKER).data("DateTimePicker").date();
			var states = [];
			var userNames = [];
			var appNames = [];
			var clientNames = [];

			if($('#state-completed').is(':checked'))
				states.push($('#state-completed').val());
			if($('#state-executing').is(':checked'))
				states.push($('#state-executing').val());
			if($('#state-init').is(':checked'))
				states.push($('#state-init').val());
			if($('#state-unknown').is(':checked'))
				states.push($('#state-unknown').val());

			var param = {};

			param.maxRows = $(FILTER_MAX_FETCH_ROWS).val();
			param.timeRange = $(FILTER_TIME_RANGE).val();
			param.startTime = startTime.format(DATE_FORMAT);
			param.endTime = endTime.format(DATE_FORMAT);
			param.states = states.join(',');
			param.queryIDs = $(FILTER_QUERY_IDS).val();
			param.userNames = $(FILTER_USER_NAMES).val();
			param.appNames = $(FILTER_APP_NAMES).val();
			param.clientNames = $(FILTER_CLIENT_NAMES).val();
			param.queryText = $(FILTER_QUERY_TEXT).val();  

			return param;
		},
		fetchQueriesInRepository: function () {
			_this.showLoading();
			$(ERROR_CONTAINER).hide();
			_this.updateFilter();
			_this.filterApplyClicked();
		},
		fetchTopN: function(){
			var timeRange = _this.getTimerange();
			var startTime = timeRange.startTime.format("YYYY-MM-DD HH:mm:ss");
			var endTime = timeRange.endTime.format("YYYY-MM-DD HH:mm:ss");
			$(CHART_CONFIG["TopN_Memory_Used"].spinner).show();
			$(CHART_CONFIG["TopN_CPU_Time"].spinner).show();
			$(CHART_CONFIG["TopN_Total_Runtime"].spinner).show();
			$(CHART_CONFIG["TopN_Disk_IO"].spinner).show();
			$(CHART_CONFIG["TopN_Memory_Used"].error).hide();
			$(CHART_CONFIG["TopN_CPU_Time"].error).hide();
			$(CHART_CONFIG["TopN_Total_Runtime"].error).hide();
			$(CHART_CONFIG["TopN_Disk_IO"].error).hide();
			wHandler.fetchTopMemUsed(startTime,endTime);
			wHandler.fetchTopCPUTime(startTime,endTime);
			wHandler.fetchTopRuntime(startTime,endTime);
			wHandler.fetchTopDiskIO(startTime,endTime);
		},
		displayTopMemUsed: function(data){
			var chartConfig = CHART_CONFIG["TopN_Memory_Used"];
			_this.displayTopCharts(data,chartConfig);
		},
		displayTopCPUTime: function(data){
			var chartConfig = CHART_CONFIG["TopN_CPU_Time"];
			_this.displayTopCharts(data,chartConfig);
		},
		displayTopRuntime: function(data){
			var chartConfig = CHART_CONFIG["TopN_Total_Runtime"];
			_this.displayTopCharts(data,chartConfig);
		},
		displayTopDiskIO: function(data){
			var chartConfig = CHART_CONFIG["TopN_Disk_IO"];
			_this.displayTopCharts(data,chartConfig);
		},
		getTimerange: function(){
			////console.log(startTime.format("YYYY-MM-DD HH:mm:ss"));
			return {
				startTime: $(START_TIME_PICKER).data("DateTimePicker").date(),
				endTime: $(END_TIME_PICKER).data("DateTimePicker").date()
			}
		},
		displayResults: function (result){
			_this.hideLoading();
			$(ERROR_CONTAINER).hide();
			$(RESULT_CONTAINER).show();

			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="repo-query-results"></table>';
				$(RESULT_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];

				$.each(result.resultArray, function(i, data){
					aaData.push(data);
				});

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var bPaging = aaData.length > 25;

				oDataTable = $('#repo-query-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "No queries found for selected time range/or filters."
					},
					//dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					dom: "<'row'<'col-md-8'lB><'col-md-4'f>>" +"<'row'<'col-md-12'<'datatable-scroll'tr>>><'row'<'col-md-12'ip>>",
					processing: true,
					paging : bPaging, 
					//autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					stateSave: true,
					select: {style: 'single', items: 'row', info: false},
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [{
						"aTargets": [ 0],
						"mData": 0,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								var rowcontent = "<a href=\"#/workloads/history/querydetail/" +
								encodeURIComponent(data)+"\">"+data+"</a>";
								return rowcontent;
							}
							else return data;
						}
					},
					{
						"aTargets": [ 2],
						"mData": 2,
						"className" : "dbmgr-nowrap",
						"mRender": function ( data, type, full ) {
							//if (type === 'display') {
							if(data != null || data != -1)
								return common.toServerLocalDateFromUtcMilliSeconds(data);
							else return "";
							//}
							//else return data;
						}
					},
					{
						"aTargets": [3 ],
						"mData": 3,
						"className" : "dbmgr-nowrap",
						"mRender": function ( data, type, full ) {
							//if (type === 'display') {
							if(data != null || data != -1)
								return common.toServerLocalDateFromUtcMilliSeconds(data);
							else return "";
							//}
							//else return data;
						}
					},
					{
						"aTargets": [7 ],
						"mData": 7,
						"mRender": function ( data, type, full ) {
							//if (type === 'display') {
							if(data != null || data != -1)
								return common.microsecondsToString(data);
							else return "";
							//}
							//else return data;
						}
					},
					{
						"aTargets": [8],
						"mData" : 8,
						"mRender": function(data, type, full){
							if(type == 'display' && data != null){
								if(data.length > 30){
									return '<div class="pointer dbmgr-text-ellipsis">'+data+'</div>';
								}else{
									return '<div class=\"dbmgr-nowrap\">'+data+'</div>';
								}
							}else
								return data;
						}
					}
					],
					buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                          // { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
	                        	   title: 'Historical Workloads' } ,
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Historical Workloads' }
				          ],
				    "order":[[2, "desc"]]
				});

				$('#repo-query-results tbody').on( 'click', 'td', function (e, a) {
					if(oDataTable.cell(this)){
						var cell = oDataTable.cell(this).index();
						if(cell){
							if(cell.column == 0){
								var data = oDataTable.row(cell.row).data();
								if(data && data.length > 0){
									sessionStorage.setItem(data[0], JSON.stringify({type: 'repo', text: data[8]}));	
								}
							}else if(cell.column == 8){
								if ($(this).find('.dbmgr-text-ellipsis').length > 0)
									$(this).find('.dbmgr-text-ellipsis').removeClass('dbmgr-text-ellipsis'); 
								else 
									$(this).find('div').addClass('dbmgr-text-ellipsis');
							}else{
								$(this).find('div').addClass('dbmgr-nowrap');
							}
						}
					}
				}).on('dblclick', 'td', function(e, a){
					if ($(this).find('.dbmgr-text-ellipsis').length > 0)
						$(this).find('.dbmgr-text-ellipsis').removeClass('dbmgr-text-ellipsis'); 
					else
						$(this).find('div').selectText();	
				});	
			}
		},
		showErrorMessage: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				_this.hideLoading();
				$(RESULT_CONTAINER).hide();
				$(ERROR_CONTAINER).show();
				if (jqXHR.responseText) {
					$(ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
					if(jqXHR.status != null && jqXHR.status == 0) {
						$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
					}
				}
			}
		},
		UTCstamp2UTCsecond: function(strDate){
			//convert UTC Date "YYYY:MM::DD HH:MM:SS" to UTC milliseconds
			//new Date() use local timezone
			var tDate = new Date(strDate);
			var utc = tDate.getTime() - tDate.getTimezoneOffset()*60000;
			return utc;
		},
		displayTopCharts: function(data,chartConfig){
			var data = data;
			var container = chartConfig.container;
			var spinner = chartConfig.spinner;
			var x_start = _this.getTimerange().startTime.unix() * 1000;
			var x_end = _this.getTimerange().endTime.unix() * 1000;
			var options = FLOT_OPTIONS;
			options.xaxis.min = x_start;
			options.xaxis.max = x_end;
			var lines = [];
			var count = 5;
			for(var i=0;i < data.length;i++){
				var status = "Complete";
				var start_time = _this.UTCstamp2UTCsecond(data[i].start_time);
				var end_time = data[i].end_time;
				if(end_time == null){
					end_time = x_end;
					status = "Executing";
				}else{
					end_time = _this.UTCstamp2UTCsecond(end_time);
				}
				var y = count;
				var value = data[i][chartConfig.value];
				var query_id = data[i].query_id;
				var line = {
						"query_id":query_id,
						"start_time":data[i].start_time,
						"end_time":data[i].end_time,
						"value": value,
						"index":y,
						"status":status,
						"data": [[start_time,y],[end_time,y]]
				}
				lines.push(line);
				count--;
			}
			$.plot("#"+container ,lines,options);
			$("#"+container).bind("plothover", function (event, pos, item) {
				if (item) {
					$("#"+container + '-tooltip').remove();
					var query_id = item.series.query_id;
					var start_time = item.series.start_time;
					var end_time = item.series.end_time;
					var index = item.series.index;
					var value = item.series.value;
					var status = item.series.status;
					var content = "Status:" + status + "</br>Value:" + value +"</br>StartTime:"+start_time+"</br>EndTime:"+end_time;
					common.showTooltip(pos.pageX, pos.pageY, content, container+'-tooltip');
				} else {
					$("#"+container + '-tooltip').remove();
				}

			});
			$(spinner).hide();
		},
		
		parseInputDate:function(date){
			return moment.tz(date, DATE_FORMAT_ZONE, common.serverTimeZone);
		},
		cancelQuery: function(){
			var selectedRows = oDataTable.rows( { selected: true } );
			if(selectedRows && selectedRows.count() >0){
				var qid = selectedRows.data()[0][0];
				$(CANCEL_QUERY_ID).text(qid);
				$(CANCEL_QUERY_DIALOG).modal('show');
			}else{
				alert("No queries were selected. Please select a query from the list.");
			}
		},
		cancelQueryConfirmed: function(){
			var qID = $(CANCEL_QUERY_ID).text();
			wHandler.cancelQuery(qID, _this);			
		},
		cancelQuerySuccess:function(data){
			if(data.requestor == _this){
				var msgText = 'The request to cancel query ' + data.queryID + ' has been submitted successfully.';
				var msgObj={msg:msgText, tag:"success", url:_this.currentURL, shortMsg:"Request to cancel query submitted successfully."};
				
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
				_this.fetchQueriesInRepository();
			}
		},
		cancelQueryError:function(jqXHR){
			if(jqXHR.requestor == _this){
				var msgObj={msg:jqXHR.responseText, tag:"danger", url:_this.currentURL, shortMsg:"Request to cancel query failed."};
				if(jqXHR.responseText==undefined){
					msgObj.msg="the response was null."
					msgObj.shortMsg="the response was null."
				}
				if(jqXHR.statusText=="abort"){
					msgObj.msg="the request was aborted."
					msgObj.shortMsg="the request was aborted."
				}
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
			}
		}
	});


	return WorkloadsView;
});
