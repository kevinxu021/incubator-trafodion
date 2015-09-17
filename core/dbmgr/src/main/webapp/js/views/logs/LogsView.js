define([
        'views/BaseView',
        'text!templates/logs.html',
        'jquery',
        'handlers/LogsHandler',
        'model/Localizer',
        'moment',
        'common',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools',
        'datetimepicker',
        'jqueryvalidate'

        ], function (BaseView, LogsT, $, logsHandler, localizer, moment, common) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
    	RESULT_CONTAINER = '#query-result-container',
    	ERROR_CONTAINER = '#errorText',
    	REFRESH_MENU = '#refreshAction',
    	DCS_LOGS = '#dcsLogs';
    
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
    var _that = null;
    var validator = null;

	var LogsView = BaseView.extend({
		template:  _.template(LogsT),

		init: function (){
			_that = this;
			
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
				_that.updateFilter();
			});
			
			$(FILTER_TIME_RANGE).change(function(){
				var sel = $(this).val();
				switch(sel){
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
			this.fetchLogs();
		},
		resume: function(){
			logsHandler.on(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.on(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			this.fetchLogs();
		},
		pause: function(){
			logsHandler.off(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.off(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
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
        },
        hideLoading: function () {
        	$(LOADING_SELECTOR).hide();
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
        	var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
        	var endTime = $('#enddatetimepicker').data("DateTimePicker").date();
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
        	
        	var param = {};
        	param.startTime = startTime.format('YYYY-MM-DD HH:mm:ss');
        	param.endTime = endTime.format('YYYY-MM-DD HH:mm:ss');
        	param.severities = severities.join(',');
        	param.componentNames = $(FILTER_COMPONENT_NAMES).val();
        	param.processNames = $(FILTER_PROCESS_NAMES).val();
        	param.errorCodes = $(FILTER_ERROR_CODES).val();
        	param.message = $(FILTER_MESSAGE_TEXT).val();
        	
        	$(FILTER_DIALOG).modal('hide');
        	$(FILTER_ERROR_MSG).html('');
        	_that.showLoading();
        	logsHandler.fetchLogs(param);
        },
        fetchLogs: function () {
			_that.showLoading();
			$(ERROR_CONTAINER).hide();
			_that.updateFilter();
			_that.filterApplyClicked();
		},

		displayResults: function (result){
			_that.hideLoading();
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
					dom: 'T<"clear">lfrtip',
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
					"tableTools": {
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},
					aaSorting: [[ 0, "desc" ]],
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             }
				});
				
				
				$('#query-results td').css("white-space","nowrap");
			}

		},
        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	$(RESULT_CONTAINER).hide();
        	$(ERROR_CONTAINER).show();
        	if (jqXHR.responseText) {
        		$(ERROR_CONTAINER).text(jqXHR.responseText);
        	}
        }  

	});


	return LogsView;
});
