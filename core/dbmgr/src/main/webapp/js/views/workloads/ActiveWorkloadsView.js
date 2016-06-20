//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

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
        'datatables.net-select',
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
	QCANCEL_MENU = '#cancelAction',
	REFRESH_INTERVAL = '#refreshInterval',
	SPINNER = '#loadingImg',
	RESULT_CONTAINER = '#active-result-container',
	ERROR_TEXT = '#active-error-text';
	
	var CANCEL_QUERY_DIALOG = '#cancel-query-dialog',
		CANCEL_QUERY_ID = '#cancel-query-id',
		CANCEL_QUERY_YES_BTN = '#cancel-query-yes-btn';
	
	var ActiveWorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			_this = this;
			refreshTimer.init();
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_ACTION).on('click', this.fetchActiveQueries);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			if(common.commonTimeRange!=null&&common.commonTimeRange.isAutoRefresh!=null){
				refreshTimer.setRefreshInterval(common.commonTimeRange.isAutoRefresh);
			}

			this.fetchActiveQueries();
		},
		doResume: function(){
			this.redirectFlag=false;
			if(common.commonTimeRange!=null&&common.commonTimeRange.isAutoRefresh!=null){
				refreshTimer.setRefreshInterval(common.commonTimeRange.isAutoRefresh);
			}
			refreshTimer.resume();
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).on('click', this.fetchActiveQueries);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			this.fetchActiveQueries();
		},
		doPause: function(){
			this.redirectFlag=true;
			if(common.commonTimeRange!=null)
				common.commonTimeRange.isAutoRefresh=$(REFRESH_INTERVAL).val();
			refreshTimer.pause();
			wHandler.off(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).off('click', this.fetchActiveQueries);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).off('click', this.cancelQueryConfirmed);
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
					//dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					dom: "<'row'<'col-md-8'lB><'col-md-4'f>>" +"<'row'<'col-md-12'<'datatable-scroll'tr>>><'row'<'col-md-12'ip>>",
					processing: true,
					paging : bPaging, 
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					stateSave: true,
					select: {style: 'single', items: 'row', info: false},
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [
					                 {
					                	 "aTargets": [ 0 ],
					                	 "mData": 0,
								 "className" : "dbmgr-nowrap",
					                	 "mRender": function ( data, type, full ) {
					                		 if (type === 'display') {
					                			 return common.toServerLocalDateFromMilliSeconds(data, 'YYYY-MM-DD HH:mm:ss');
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
					                			 encodeURIComponent(data)+"\">"+data+"</a>";
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
				          ]
				});

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
		},
		cancelQuery: function(){
			var selectedRows = oDataTable.rows( { selected: true } );
			if(selectedRows && selectedRows.count() >0){
				var qid = selectedRows.data()[0][2];
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
				var msgObj={ msg:msgText, tag:"success", url:_this.currentURL, shortMsg:"Request to cancel query submitted successfully."};
				
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
				_this.fetchActiveQueries();
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

	return ActiveWorkloadsView;
});
