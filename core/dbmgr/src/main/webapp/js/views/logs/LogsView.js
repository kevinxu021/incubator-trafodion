// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/logs.html',
        'jquery',
        'handlers/LogsHandler',
        'model/Localizer',
        'moment',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tablebuttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',     
        'datetimepicker',
        'jqueryvalidate'

        ], function (BaseView, LogsT, $, logsHandler, localizer, moment, common, refreshTimerView) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
    	RESULT_CONTAINER = '#query-result-container',
    	ERROR_CONTAINER = '#errorText',
    	REFRESH_MENU = '#refreshAction',
    	DCS_LOGS = '#dcsLogs',
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
		FILTER_COMPONENT_NAMES = '#filter-component-names',
		FILTER_PROCESS_NAMES = '#filter-process-names',
		FILTER_ERROR_CODES = '#filter-error-codes',
		FILTER_MESSAGE_TEXT = '#filter-message-text',
		FILTER_ERROR_MSG = '#filter-error-text',
		FILTER_TIME_RANGE = '#filter-time-range';
    
    var oDataTable = null;
    var _this = null;
    var validator = null;

	var LogsView = BaseView.extend({
		template:  _.template(LogsT),

		doInit: function (){
			_this = this;
			
			$.validator.addMethod("validateStartAndEndTimes", function(value, element) {
				var startTime = new Date($(START_TIME_PICKER).data("DateTimePicker").date()).getTime();
				var endTime = new Date($(END_TIME_PICKER).data("DateTimePicker").date()).getTime();
				return (startTime < endTime);
			}, "* Start Time has to be less than End Time");
			
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
			
			$(START_TIME_PICKER).datetimepicker({format: 'YYYY-MM-DD HH:mm:ss z'});
			$(END_TIME_PICKER).datetimepicker({format: 'YYYY-MM-DD HH:mm:ss z'});
			$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				_this.updateFilter();
			});
			
			$(FILTER_TIME_RANGE).change(function(){
				var sel = $(this).val();
				switch(sel){
				case "1":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "6":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "24":
					$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
					$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "0":
					$('#filter-start-time').prop("disabled", false);
					$('#filter-end-time').prop("disabled", false);
				}
			});
			if(common.dcsMasterInfoUri != null && common.dcsMasterInfoUri.length > 0)
				$(DCS_LOGS).html('<a href="' + common.dcsMasterInfoUri+'" target="_blank">DCS Logs</a>');
			else{
				$(DCS_LOGS).html('');
			}
			logsHandler.on(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.on(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);		
			$(REFRESH_MENU).on('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			
			refreshTimerView.init();
			refreshTimerView.eventAgg.on(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.on(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
			refreshTimerView.setRefreshInterval(1);
			
			this.fetchLogs();
		},
		doResume: function(){
			logsHandler.on(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.on(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			refreshTimerView.eventAgg.on(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.on(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
			this.fetchLogs();
		},
		doPause: function(){
			refreshTimerView.pause();
			logsHandler.off(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.off(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
			refreshTimerView.eventAgg.off(refreshTimerView.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimerView.eventAgg.off(refreshTimerView.events.INTERVAL_CHANGED, this.timerBeeped);
		},
		dcsLogsClicked: function(){
			
		},
        showLoading: function(){
        	$(LOADING_SELECTOR).show();
        },
        updateFilter: function(){
        	var selection = $(FILTER_TIME_RANGE).val();
			switch(selection){
			case "1":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "6":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(6, 'hour'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "24":
				$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'day'));
				$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "0":
				$('#filter-start-time').prop("disabled", false);
				$('#filter-end-time').prop("disabled", false);
			}   
			_this.updateTimeRangeLabel();
        },
        hideLoading: function () {
        	$(LOADING_SELECTOR).hide();
        },
		updateTimeRangeLabel: function(){
			$(TIME_RANGE_LABEL).text('Time Range : ' +  $(START_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE) + ' - ' +  $(END_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE));
		},
        filterButtonClicked: function(){
        	$(FILTER_ERROR_MSG).html('');
        	$(FILTER_DIALOG).modal('show');
        },
        filterApplyClicked: function(){
        	if($(FILTER_FORM).valid()){
        		
        	}else{
        		return;
        	}
        	_this.updateTimeRangeLabel();
        	var startTime = $(START_TIME_PICKER).data("DateTimePicker").date();
        	var endTime = $(END_TIME_PICKER).data("DateTimePicker").date();
        	var severities = [];
        	var components = [];
        	var processNames = [];
        	var errorCodes = [];
        	
        	if($('#severity-fatal').is(':checked'))
        		severities.push($('#severity-fatal').val());
        	if($('#severity-error').is(':checked'))
        		severities.push($('#severity-error').val());
        	if($('#severity-warn').is(':checked'))
        		severities.push($('#severity-warn').val());
        	if($('#severity-info').is(':checked'))
        		severities.push($('#severity-info').val());
        	if($('#severity-debug').is(':checked'))
        		severities.push($('#severity-debug').val());
        	
        	if($('#component-mon').is(':checked'))
        		components.push($('#component-mon').val());
        	if($('#component-mxosrvr').is(':checked'))
        		components.push($('#component-mxosrvr').val());
        	if($('#component-sql').is(':checked'))
        		components.push($('#component-sql').val());
        	if($('#component-sql-comp').is(':checked'))
        		components.push($('#component-sql-comp').val());
        	if($('#component-sql-descgen').is(':checked'))
        		components.push($('#component-sql-descgen').val());
        	if($('#component-sql-esp').is(':checked'))
        		components.push($('#component-sql-esp').val());
        	if($('#component-sql-exe').is(':checked'))
        		components.push($('#component-sql-exe').val());
        	if($('#component-sql-lob').is(':checked'))
        		components.push($('#component-sql-lob').val());
        	if($('#component-sql-sscp').is(':checked'))
        		components.push($('#component-sql-sscp').val());
        	if($('#component-sql-ssmp').is(':checked'))
        		components.push($('#component-sql-ssmp').val());
        	if($('#component-sql-udr').is(':checked'))
        		components.push($('#component-sql-udr').val());
        	if($('#component-wdg').is(':checked'))
        		components.push($('#component-wdg').val());
        	
        	var param = {};
        	param.startTime = startTime.format('YYYY-MM-DD HH:mm:ss');
        	param.endTime = endTime.format('YYYY-MM-DD HH:mm:ss');
        	param.severities = severities.join(',');
        	param.componentNames = components.join(',');
        	param.processNames = $(FILTER_PROCESS_NAMES).val();
        	param.errorCodes = $(FILTER_ERROR_CODES).val();
        	param.message = $(FILTER_MESSAGE_TEXT).val();
        	
        	$(FILTER_DIALOG).modal('hide');
        	$(FILTER_ERROR_MSG).html('');
        	_this.showLoading();
        	logsHandler.fetchLogs(param);
        },
        refreshIntervalChanged: function(){

        },
        timerBeeped: function(){
        	_this.fetchLogs();
        },
        fetchLogs: function () {
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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="query-results"></table>';
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

				oDataTable = $('#query-results').dataTable({
					"oLanguage": {
       				 "sEmptyTable": "No log entries found."
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					"bProcessing": true,
					"bPaginate" : true, 
					//"bAutoWidth": true,
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					//stateSave: true,
					"aoColumnDefs": [ {
					      "aTargets": [ 0 ],
					      "mData": 0,
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					          return moment(data).format("YYYY-MM-DD HH:mm:ss");
					        }
					        else return data;
					      }
					    } ],
					paging: true,
					buttons: [
					          'copy','csv','excel','pdf','print'
				          ],
					aaSorting: [[ 0, "desc" ]],
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             }
				});
				
				
				$('#query-results td').css("white-space","nowrap");
			}

		},
        showErrorMessage: function (jqXHR) {
        	_this.hideLoading();
        	$(RESULT_CONTAINER).hide();
        	$(ERROR_CONTAINER).show();
        	if (jqXHR.responseText) {
        		$(ERROR_CONTAINER).text(jqXHR.responseText);
        	}
        }  

	});


	return LogsView;
});
