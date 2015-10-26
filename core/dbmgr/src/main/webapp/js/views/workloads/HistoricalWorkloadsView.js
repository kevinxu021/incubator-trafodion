// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workloads_history.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'datetimepicker',
        'jqueryvalidate',
        'tablebuttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml'        
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
		RESULT_CONTAINER = '#query-result-container',
		ERROR_CONTAINER = '#errorText',
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
		FILTER_TIME_RANGE = '#filter-time-range';
    
    var oDataTable = null;
    var _this = null;
    var validator = null;
    
	var WorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

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
			
			
			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			this.fetchQueriesInRepository();
		},
		doResume: function(){
			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			this.fetchQueriesInRepository();
		},
		doPause: function(){
			wHandler.off(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).off('click', this.fetchLogs);
			$(FILTER_APPLY_BUTTON).off('click', this.filterApplyClicked);
			$(OPEN_FILTER).off('click', this.filterButtonClicked);
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
		updateTimeRangeLabel: function(){
			$(TIME_RANGE_LABEL).text('Time Range : ' +  $(START_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE) + ' - ' +  $(END_TIME_PICKER).data("DateTimePicker").date().format(DATE_FORMAT_ZONE));
		},
		filterButtonClicked: function(){
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
        	param.startTime = startTime.format('YYYY-MM-DD HH:mm:ss');
        	param.endTime = endTime.format('YYYY-MM-DD HH:mm:ss');
        	param.states = states.join(',');
        	param.queryIDs = $(FILTER_QUERY_IDS).val();
        	param.userNames = $(FILTER_USER_NAMES).val();
        	param.appNames = $(FILTER_APP_NAMES).val();
        	param.clientNames = $(FILTER_CLIENT_NAMES).val();
        	param.queryText = $(FILTER_QUERY_TEXT).val();
        	
        	$(FILTER_DIALOG).modal('hide');
        	_this.showLoading();
        	wHandler.fetchQueriesInRepository(param);
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
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="query-results"></table>';
				$('#query-result-container').html( sb );

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
       				 "sEmptyTable": "No queries found."
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					"bProcessing": true,
					"bPaginate" : bPaging, 
					"bAutoWidth": false,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
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
						    }
						    ],
					paging: true,
					buttons: [
					          'copy','csv','excel','pdf','print'
				          ],					                 
					fnDrawCallback: function(){
						//$('#query-results td').css("white-space","nowrap");
		             }
				});
				
				//$('#query-results td').css("white-space","nowrap");
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


	return WorkloadsView;
});
