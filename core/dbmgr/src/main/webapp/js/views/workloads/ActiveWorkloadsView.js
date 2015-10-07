// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/active_workloads.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools'
        ], function (BaseView, WorkloadsT, $, wHandler, common, refreshTimer) {
	'use strict';
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var oDataTable = null;
    var _this = null;
    var REFRESH_ACTION = '#refreshAction',
    	REFRESH_INTERVAL = '#refreshInterval',
    	SPINNER = '#loadingImg',
    	ERROR_TEXT = '#errorText';
    
	var ActiveWorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			_this = this;
			refreshTimer.init();
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);
			$(REFRESH_ACTION).on('click', this.fetchActiveQueries);
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.setRefreshInterval(0.5);
			
			this.fetchActiveQueries();
		},
		doResume: function(){
			refreshTimer.resume();
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).on('click', this.fetchActiveQueries);
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			this.fetchActiveQueries();
		},
		doPause: function(){
			refreshTimer.pause();
			wHandler.off(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).off('click', this.fetchActiveQueries);
			refreshTimer.eventAgg.off(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
		},
        showLoading: function(){
        	$(SPINNER).show();
        },

        hideLoading: function () {
        	$(SPINNER).hide();
        },
        refreshIntervalChanged: function(){

        },
        timerBeeped: function(){
        	_this.fetchActiveQueries();
        },
        fetchActiveQueries: function () {
			_this.showLoading();
			wHandler.fetchActiveQueries();
		},

		displayResults: function (result){
			_this.hideLoading();
			$(ERROR_TEXT).hide();
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
					"aoColumnDefs": [
					    {
					      "aTargets": [ 0 ],
					      "mData": 0,
					      "mRender": function ( data, type, full ) {
					       if (type === 'display') {
					          return common.toDateFromMilliSeconds(data);
					        }
					        else return data;
					      }
					    },
					    {
						      "aTargets": [ 2],
						      "mData": 2,
						      "mRender": function ( data, type, full ) {
						       if (type === 'display') {
						    	   var rowcontent = "<a href=\"#/workloads/active/querydetail/" +
	                       			data+"\">"+data+"</a>";
	                       			return rowcontent;
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
				
				$('#query-results td').css("white-space","nowrap");
			}

		},
        showErrorMessage: function (jqXHR) {
        	_this.hideLoading();
        	$(ERROR_TEXT).show();
        	if (jqXHR.responseText) {
        		$(ERROR_TEXT).text(jqXHR.responseText);
        	}
        }  
	});

	return ActiveWorkloadsView;
});
