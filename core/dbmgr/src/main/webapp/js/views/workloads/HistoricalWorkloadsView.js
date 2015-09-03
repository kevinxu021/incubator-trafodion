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
        'tabletools',
        'datetimepicker',
        'jqueryvalidate'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
		RESULT_CONTAINER = '#query-result-container',
		ERROR_CONTAINER = '#errorText',
  		REFRESH_MENU = '#refreshAction';
    
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
    var _that = null;
    var validator = null;
    
	var WorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

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
			$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(1, 'hour'));
			$('#enddatetimepicker').data("DateTimePicker").date(moment());

			$(FILTER_DIALOG).on('show.bs.modal', function (e) {
				_that.updateFilter();
			});
			
			$(FILTER_TIME_RANGE).change(function(){
				var sel = $(this).val();
				switch(sel){
				case "1":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(1, 'hour'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment());
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "6":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(6, 'hour'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment());
					$('#filter-start-time').prop("disabled", true);
					$('#filter-end-time').prop("disabled", true);
					break;
				case "24":
					$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(1, 'day'));
					$('#enddatetimepicker').data("DateTimePicker").date(moment());
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
		resume: function(){
			wHandler.on(wHandler.FETCH_REPO_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_ERROR, this.showErrorMessage);			
			$(REFRESH_MENU).on('click', this.fetchQueriesInRepository);
			$(FILTER_APPLY_BUTTON).on('click', this.filterApplyClicked);
			$(OPEN_FILTER).on('click', this.filterButtonClicked);
			this.fetchQueriesInRepository();
		},
		pause: function(){
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
				$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(1, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment());
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "6":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(6, 'hour'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment());
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "24":
				$('#startdatetimepicker').data("DateTimePicker").date(moment().subtract(1, 'day'));
				$('#enddatetimepicker').data("DateTimePicker").date(moment());
				$('#filter-start-time').prop("disabled", true);
				$('#filter-end-time').prop("disabled", true);
				break;
			case "0":
				$('#filter-start-time').prop("disabled", false);
				$('#filter-end-time').prop("disabled", false);
			}        	
        },
        filterButtonClicked: function(){
        	$(FILTER_DIALOG).modal('show');
        },
        filterApplyClicked: function(){
        	if($(FILTER_FORM).valid()){
        		
        	}else{
        		return;
        	}
        	var startTime = $('#startdatetimepicker').data("DateTimePicker").date();
        	var endTime = $('#enddatetimepicker').data("DateTimePicker").date();
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

        	wHandler.fetchQueriesInRepository(param);
        },
        fetchQueriesInRepository: function () {
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
					dom: 'T<"clear">lfrtip',
					"bProcessing": true,
					"bPaginate" : bPaging, 
					"bAutoWidth": true,
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
					      "aTargets": [ 2,3 ],
					      "mData": 2,
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					          return common.toDateFromMilliSeconds(data);
					        }
					        else return data;
					      },
					      "mData": 3,
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					          return common.toDateFromMilliSeconds(data);
					        }
					        else return data;
					      },
					    } ],
					paging: true,
					"tableTools": {
						"sRowSelect": "multi",
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},
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


	return WorkloadsView;
});
