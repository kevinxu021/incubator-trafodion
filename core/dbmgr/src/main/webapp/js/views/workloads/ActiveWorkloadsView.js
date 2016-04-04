//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/active_workloads.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkloadsT, $, wHandler, common, refreshTimer) {
	'use strict';
	var LOADING_SELECTOR = ".dbmgr-spinner";			
	var oDataTable = null;
	var _this = null;
	var REFRESH_ACTION = '#refreshAction',
	REFRESH_INTERVAL = '#refreshInterval',
	SPINNER = '#loadingImg',
	RESULT_CONTAINER = '#active-result-container',
	ERROR_TEXT = '#active-error-text';

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
			$(RESULT_CONTAINER).show();
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="active-query-results"></table>';
				$('#active-result-container').html( sb );

				
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

				oDataTable = $('#active-query-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "No active queries found."
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging : bPaging, 
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					stateSave: true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [
					                 {
					                	 "aTargets": [ 0 ],
					                	 "mData": 0,
								 "className" : "dbmgr-nowrap",
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
					buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                          // { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
	                        	   title: 'Active Workloads' } ,
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Active Workloads' }
				          ],

	                           fnDrawCallback: function(){
	                        	   //$('#query-results td').css("white-space","nowrap");
	                           }
				});

				//$('#active-query-results td').css("white-space","nowrap");
				$('#active-query-results tbody').on( 'click', 'td', function (e, a) {
					if(oDataTable.cell(this)){
						var cell = oDataTable.cell(this).index();
						if(cell){
							if(cell.column == 2){
								var data = oDataTable.row(cell.row).data();
								if(data && data.length > 0){
									sessionStorage.setItem(data[2], JSON.stringify({type: 'active', text: data[4]}));
								}
							}
						}
					}
				});	
			}
		},
		showErrorMessage: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				_this.hideLoading();
				$(ERROR_TEXT).show();
				$(RESULT_CONTAINER).hide();
				if (jqXHR.responseText) {
					$(ERROR_TEXT).text(jqXHR.responseText);
				}else{
	        		if(jqXHR.status != null && jqXHR.status == 0) {
	        			$(ERROR_TEXT).text("Error : Unable to communicate with the server.");
	        		}
	        	}
			}
		}  
	});

	return ActiveWorkloadsView;
});
