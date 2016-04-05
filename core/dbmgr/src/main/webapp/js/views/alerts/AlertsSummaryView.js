//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/alerts_summary.html',
        'jquery',
        'handlers/ServerHandler',
        'moment',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',        
        'datetimepicker',
        'jqueryvalidate',
        'pdfmake'
        ], function (BaseView, AlertsT, $, serverHandler, moment, common, refreshTimerView) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	RESULT_CONTAINER = '#alerts-result-container',
	ERROR_CONTAINER = '#alerts-list-error',
	REFRESH_MENU = '#refreshAction',
	REFRESH_INTERVAL = '#refreshInterval';

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
	FILTER_ALERT_TEXT = '#filter-alert-text',
	FILTER_TIME_RANGE = '#filter-time-range';

	var oDataTable = null;
	var _this = null;
	var validator = null;
	var lastAppliedFilters = null; //last set of filters applied by user explicitly

	var AlertsSummaryView = BaseView.extend({
		template:  _.template(AlertsT),

		doInit: function (){
			_this = this;
			
			this.bindInitialEvents();
			$(START_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			$(END_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			this.initialTimeRangePicker();
			$(START_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			$(END_TIME_PICKER).datetimepicker({format: DATE_FORMAT_ZONE, sideBySide:true, showTodayButton: true, parseInputDate: _this.parseInputDate});
			$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));

			serverHandler.on(serverHandler.FETCH_ALERTS_LIST_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCH_ALERTS_LIST_ERROR, this.showErrorMessage);
			$(REFRESH_MENU).on('click', this.fetchAlertsSummary);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			refreshTimerView.init();
			refreshTimerView.eventAgg.on(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.on(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
			refreshTimerView.setRefreshInterval(1);
			this.fetchAlertsSummary();
		},
		doResume: function(){
			this.initialTimeRangePicker();
			serverHandler.on(serverHandler.FETCH_ALERTS_LIST_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCH_ALERTS_LIST_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchAlertsSummary);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			refreshTimerView.eventAgg.on(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.on(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
			this.fetchAlertsSummary();
		},
		doPause: function(){
			this.storeCommonTimeRange();
			serverHandler.off(serverHandler.FETCH_ALERTS_LIST_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.FETCH_ALERTS_LIST_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
			refreshTimerView.eventAgg.off(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.off(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
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
				$(FILTER_TIME_RANGE).val(common.commonTimeRange.timeRangeTag);
			}
			lastAppliedFilters =  _this.getFilterParams();
		},
		bindInitialEvents:function(){
			$.validator.addMethod("validateCustomStartTime", function(value, element) {
				var timeRange = $(FILTER_TIME_RANGE).val();
				if(timeRange == '0'){
					var sDate = $(START_TIME_PICKER).data("DateTimePicker").date();
					return sDate != null;
				}
				return true;

			}, "* Start Time is required for custom time range.");

			$.validator.addMethod("validateCustomEndTime", function(value, element) {
				var timeRange = $(FILTER_TIME_RANGE).val();
				if(timeRange == '0'){
					var eDate = $(END_TIME_PICKER).data("DateTimePicker").date();
					return eDate != null;
				}
				return true;

			}, "* End Time is required for custom time range.");

			$.validator.addMethod("validateStartAndEndTimes", function(value, element) {
				var timeRange = $(FILTER_TIME_RANGE).val();
				if(timeRange == '0'){
					var sDate = $(START_TIME_PICKER).data("DateTimePicker").date();
					var eDate = $(END_TIME_PICKER).data("DateTimePicker").date();
					if(sDate != null && eDate != null){
						var startTime = new Date(sDate).getTime();
						var endTime = new Date(eDate).getTime();
						return (startTime > 0 && startTime < endTime);					
					}
					return false;
				}
				return true;

			}, "* Invalid Date Time and/or Start Time is greater than End Time");
			validator = $(FILTER_FORM).validate({
				rules: {
					"filter-start-time": { required: false, validateCustomStartTime: true, validateStartAndEndTimes: true },
					"filter-end-time": { required: false, validateCustomEndTime: true, validateStartAndEndTimes: true }
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
			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				_this.updateFilter();
			});

			$(FILTER_DIALOG).on('hide.bs.modal', function (e, v) {
				if(document.activeElement != $(FILTER_APPLY_BUTTON)[0]){
					validator.resetForm();
					_this.resetFilter(); //cancel clicked
				}
			});

			$(FILTER_TIME_RANGE).change(function(){
				var sel = $(this).val();
				switch(sel){
				case "-1":
					$('#startdatetimepicker').data("DateTimePicker").clear();
					$('#enddatetimepicker').data("DateTimePicker").clear();
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "1":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "6":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "24":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "0":
					$('#filter-start-time').prop("disabled", false);
					$('#filter-end-time').prop("disabled", false);
				}
			});
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
			case "-1":
				$('#startdatetimepicker').data("DateTimePicker").clear();
				$('#enddatetimepicker').data("DateTimePicker").clear();
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "1":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "6":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "24":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "128":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'week'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "0":
				$('#filter-start-time').prop("disabled", false);
				$('#filter-end-time').prop("disabled", false);
			}        	
			_this.updateTimeRangeLabel();
		},
		updateTimeRangeLabel: function(){
			var timeRange = $(FILTER_TIME_RANGE).val();
			if(timeRange != null) {
				if(timeRange != '-1'){
					$(TIME_RANGE_LABEL).text('Time Range : ' +  $(START_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE) + ' - ' +  $(END_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE));
				}else{
					$(TIME_RANGE_LABEL).text('Time Range : <All time ranges included>' );
				}
			}
		},
		storeCommonTimeRange:function(){
			var selection = $(FILTER_TIME_RANGE).val();
			if(selection != null) {
				if(selection != '-1'){
					common.getCommonTimeRange(selection);
				}else{
					common.getCommonTimeRange("1");
				}
			}
		},
		filterButtonClicked: function(){
			$(FILTER_DIALOG).modal('show');
		},
		resetFilter:function(){
			$(FILTER_ALERT_TEXT).val('');

			$('#state-ack').prop('checked',false);
			$('#state-unack').prop('checked', false);
			$('#state-init').prop('checked', false)
			
			$('#severity-crit').prop('checked',false);
			$('#severity-warn').prop('checked', false);
			$('#severity-normal').prop('checked', false)
			$('#severity-unknown').prop('checked', false);

			if(lastAppliedFilters != null){
				$(FILTER_TIME_RANGE).val(lastAppliedFilters.timeRange);
				if(lastAppliedFilters.timeRange == '0'){
					$(START_TIME_PICKER).data("DateTimePicker").date(moment(lastAppliedFilters.startTime));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment(lastAppliedFilters.endTime));
				}
				$(FILTER_ALERT_TEXT).val(lastAppliedFilters.alertText);  

				var states = lastAppliedFilters.filter.split(' ');
				$.each(states, function(index, value){
					var parts = value.split(':');
					if(parts.length > 1){
						if(parts[0] == 'status'){
							if($('#severity-' + part[1].toLowerCase()))
								$('#severity-' + part[1].toLowerCase()).prop('checked', true);
						}else{
							if(parts[1] == 'true')
								$('#state-ack').prop('checked', true);
							else
								$('#state-unack').prop('checked', true);
						}

					}
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
			serverHandler.fetchAlertsList(param);
		},
		getFilterParams: function(){
			var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
			var endTime = $('#enddatetimepicker').data("DateTimePicker").date();
			var filter = [];

			if($('#state-ack').is(':checked'))
				filter.push('ack:true');
			if($('#state-unack').is(':checked'))
				filter.push('ack:false');

			if($('#severity-crit').is(':checked'))
				filter.push('status:'+$('#severity-crit').val());
			if($('#sfilterverity-warn').is(':checked'))
				filter.push('status:'+$('#severity-warn').val());
			if($('#severity-normal').is(':checked'))
				filter.push('status:'+$('#severity-normal').val());
			if($('#severity-unknown').is(':checked'))
				filter.push('status:'+$('#severity-unknown').val());

			var param = {};
			param.timeRange = $(FILTER_TIME_RANGE).val();
			if(startTime){
				param.startTime = startTime.format('YYYY-MM-DD HH:mm:ss');
			}
			if(endTime){
				param.endTime = endTime.format('YYYY-MM-DD HH:mm:ss');
			}
			param.filter = filter.join(' ');
			param.alertText = $(FILTER_ALERT_TEXT).val();
			return param;
		},
		refreshIntervalChanged: function(){

		},
		timerBeeped: function(){
			_this.fetchAlertsSummary();
		},
		fetchAlertsSummary: function () {
			_this.showLoading();
			$(ERROR_CONTAINER).hide();
			_this.updateFilter();
			_this.filterApplyClicked();
		},

		displayResults: function (result){
			_this.hideLoading();
			$(ERROR_CONTAINER).hide();
			$(RESULT_CONTAINER).show();

			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="alerts-results"></table>';
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

				oDataTable = $('#alerts-results').dataTable({
					"oLanguage": {
						"sEmptyTable": "No alerts found for selected time range and/or filters."
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					"bProcessing": true,
					"bPaginate" : bPaging, 
					"bAutoWidth": true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
					stateSave: true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [{
						"aTargets": [ 0],
						"mData": 0,
						"mRender": function ( data, type, full ) {
							//if (type === 'display') {
							//if(data != null || data != -1)
							//return common.toServerLocalDateFromUtcMilliSeconds(data);
							//else return "";
							//}
							return data;
						}
					},
					{
						"aTargets": [ 1],
						"mData": 1,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								var rowcontent = "<a href=\"#/alert/detail/" +
								data+"\">"+data+"</a>";
								return rowcontent;
							}
							else return data;
						}
					} ],
					paging: true,
					buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           //{ extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
	                        	   title: 'Alerts' } ,
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Alerts' }
				          ],
				    fnDrawCallback: function(){
					        	  //$('#alerts-results td').css("white-space","nowrap");
					          }
				});

				$('#alerts-results td').css("white-space","nowrap");
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
		parseInputDate:function(date){
			return moment.tz(date, DATE_FORMAT_ZONE, common.serverTimeZone);
		}  
	});


	return AlertsSummaryView;
});
