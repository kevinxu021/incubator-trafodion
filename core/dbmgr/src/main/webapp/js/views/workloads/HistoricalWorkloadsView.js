//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

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
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	RESULT_CONTAINER = '#repo-result-container',
	ERROR_CONTAINER = '#repo-error-text',
	REFRESH_MENU = '#refreshAction';

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

	var oDataTable = null;
	var _this = null;
	var validator = null;
	var resizeTimer = null;
	var lastAppliedFilters = null; //last set of filters applied by user explicitly

	var WorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;

			$.validator.addMethod("validateStartAndEndTimes", function(value, element) {
				var startTime = new Date($(START_TIME_PICKER).data("DateTimePicker").date()).getTime();
				var endTime = new Date($(END_TIME_PICKER).data("DateTimePicker").date()).getTime();
				return (startTime > 0 && startTime < endTime);
			}, "* Invalid Date Time and/or Start Time is not less than End Time");

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
			$(START_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone).subtract(1, 'hour'));
			$(END_TIME_PICKER).data("DateTimePicker").date(moment().tz(common.serverTimeZone));

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
				case "0":
					$(FILTER_START_TIME).prop("disabled", false);
					$(FILTER_END_TIME).prop("disabled", false);
				}
			});

			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			$(window).on('resize', this.onResize);
			this.fetchQueriesInRepository();
		},
		doResume: function(){
			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			$(window).on('resize', this.onResize);
			//this.fetchQueriesInRepository();
		},
		doPause: function(){
			wHandler.off(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
			$(window).off('resize', this.onResize);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},
		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		updateFilter: function(){
			var selection = $(FILTER_TIME_RANGE).val();
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
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging : bPaging, 
					//autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					stateSave: true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [{
						"aTargets": [ 0],
						"mData": 0,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								var rowcontent = "<a href=\"#/workloads/history/querydetail/" +
								data+"\">"+data+"</a>";
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
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
	                        	   title: 'Historical Workloads' } ,
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Historical Workloads' }
				          ],
				    "order":[[2, "desc"]],
					          fnDrawCallback: function(){
					        	  //$('#repo-query-results td').css("white-space","nowrap");
					          }
				});

				//$('#repo-query-results td').css("white-space","nowrap");
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
		parseInputDate:function(date){
			return moment.tz(date, DATE_FORMAT_ZONE, common.serverTimeZone);
		},
		onResize: function() {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doResize, 200);
		},
		doResize: function() {
				if(oDataTable != null){
					oDataTable.columns.adjust().draw();
				}

		}		
	});


	return WorkloadsView;
});
