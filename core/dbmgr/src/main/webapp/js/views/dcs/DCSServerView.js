//@@@ START COPYRIGHT @@@
//
//(C) Copyright 2016 Esgyn Corporation
//
//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/dcsservers.html',
        'jquery',
        'handlers/ServerHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'datatables.net-select',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, DcsServerT, $, serverHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg',
	SUMMARY_LOADING_SELECTOR = '#summaryloadingImg',
	PSTACK_LOADING_SELECTOR = '#pstackLloadingImg',
	SUMMARY_CONTAINER = '#dcs-summary-container',
	SUMMARY_ERROR_CONTAINER = '#dcs-summary-error-text',
	RESULT_CONTAINER = '#dcs-result-container',
	ERROR_CONTAINER = '#dcs-error-text',
	REFRESH_ACTION = '#refreshAction',
	PSTACK_ACTION = '#pstackAction',
	PSTACK_DIALOG = '#pStackDialog',
	PSTACK_DAILOG_LABEL = '#pStackDialogLabel',
	PSTACK_CONTAINER = '#pStackContainer';

	var oDataTable = null;
	var _that = null;

	var DCSServerView = BaseView.extend({
		template:  _.template(DcsServerT),

		doInit: function (){
			_that = this;
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			serverHandler.on(serverHandler.DCS_SUMMARY_SUCCESS, this.displaySummary);
			serverHandler.on(serverHandler.DCS_SUMMARY_ERROR, this.showSummaryErrorMessage);			

			serverHandler.on(serverHandler.PSTACK_SUCCESS, this.displayPStack);
			serverHandler.on(serverHandler.PSTACK_ERROR, this.displayPStack);			

			$(REFRESH_ACTION).on('click', this.fetchDcsServers);
			$(PSTACK_ACTION).on('click', this.getPStack);
			$(PSTACK_DIALOG).on('show.bs.modal', function () {
			       $(this).find('.modal-body').css({
			              width:'auto', //probably not needed
			              height:'auto', //probably not needed 
			              'max-height':'100%'
			       });
	        	});
			this.fetchDcsServers();
		},
		doResume: function(){
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			serverHandler.on(serverHandler.PSTACK_SUCCESS, this.displayPStack);
			serverHandler.on(serverHandler.PSTACK_ERROR, this.displayPStack);			
			serverHandler.on(serverHandler.DCS_SUMMARY_SUCCESS, this.displaySummary);
			serverHandler.on(serverHandler.DCS_SUMMARY_ERROR, this.showSummaryErrorMessage);			
			$(REFRESH_ACTION).on('click', this.fetchDcsServers);
			$(PSTACK_ACTION).on('click', this.getPStack);
			this.fetchDcsServers();
		},
		doPause: function(){
			serverHandler.off(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			serverHandler.off(serverHandler.PSTACK_SUCCESS, this.displayPStack);
			serverHandler.off(serverHandler.PSTACK_ERROR, this.displayPStack);			
			serverHandler.off(serverHandler.DCS_SUMMARY_SUCCESS, this.displaySummary);
			serverHandler.off(serverHandler.DCS_SUMMARY_ERROR, this.showSummaryErrorMessage);			
			$(REFRESH_ACTION).off('click', this.fetchDcsServers);
			$(PSTACK_ACTION).off('click', this.getPStack);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		fetchDcsServers: function () {
			_that.showLoading();
			$(SUMMARY_LOADING_SELECTOR).show();
			$(ERROR_CONTAINER).hide();
			$(SUMMARY_ERROR_CONTAINER).hide();
			serverHandler.fetchDcsServers();
			serverHandler.fetchDcsSummary();
		},
		getPStack: function(){
			//serverHandler.getPStack();
			var selectedRows = oDataTable.rows( { selected: true } );
			if(selectedRows && selectedRows.count() >0){
				var processID = selectedRows.data()[0][6];
				var processName = selectedRows.data()[0][7];
				$(PSTACK_CONTAINER).val("Fetching pstack information ...");
	        	$(PSTACK_DIALOG).modal('show');
	        	$(PSTACK_LOADING_SELECTOR).show();
	        	$(PSTACK_DAILOG_LABEL).text("PStack for " + processName);
				serverHandler.getPStack(processID, processName);
			}else{
				alert("No master executed process selected. Please select a master executor process from the list and try again.");
			}
		},
		displayPStack: function(result){
			$(PSTACK_LOADING_SELECTOR).hide();
			$(PSTACK_CONTAINER).val(result.pStack.replace(/\\n/g,'\r\n'));
 		},
 		displaySummary: function(result){
 			$(SUMMARY_LOADING_SELECTOR).hide();
 			$(SUMMARY_CONTAINER).show();
 			$(SUMMARY_ERROR_CONTAINER).hide();
			$(SUMMARY_CONTAINER).empty();
			for (var property in result) {
				var value = result[property];
				$(SUMMARY_CONTAINER).append('<tr><td style="padding:3px 0px;width:250px">' + property + '</td><td>' + value +  '</td>');
			}
			
 		},
		displayResults: function (result){
			_that.hideLoading();
			$(ERROR_CONTAINER).hide();
			$(RESULT_CONTAINER).show();
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="dcs-query-results"></table>';
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

<<<<<<< HEAD
				oDataTable = $('#dcs-query-results').dataTable({
=======
				oDataTable = $('#dcs-query-results').DataTable({
>>>>>>> 6a8ca78bfeae73b51ba6a11c9b731d4e4127cd79
					"oLanguage": {
						"sEmptyTable": "There are no dcs servers"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
<<<<<<< HEAD
					"bProcessing": true,
					paging: bPaging,
					//"bAutoWidth": true,
=======
					processing: true,
					paging: bPaging,
					//autoWidth: true,
>>>>>>> 6a8ca78bfeae73b51ba6a11c9b731d4e4127cd79
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
					select: {style: 'single', items: 'row', info: false},
					stateSave: true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ {
						"aTargets": [ 2 ],
						"mData": 2,
						"mRender": function ( data, type, full ) {
							return moment(data , 'ddd MMM DD HH:mm:ss Z YYYY').format('YYYY-MM-DD HH:mm:ss');
						}
					} ],
					buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
	                        	   title: 'Connectivity Servers'} ,
	                           { extend : 'print', orientation: 'landscape', exportOptions: { columns: ':visible' }, title: 'Connectivity Servers' }
				          ],
					fnDrawCallback: function(){
						$('#dcs-query-results td').css("white-space","nowrap");
					}
				});
			}
		},
		showErrorMessage: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				_that.hideLoading();
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
		showSummaryErrorMessage: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				$(SUMMARY_LOADING_SELECTOR).hide();
				$(SUMMARY_CONTAINER).hide();
				$(SUMMARY_ERROR_CONTAINER).show();
				if (jqXHR.responseText) {
					$(SUMMARY_ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
	        		if(jqXHR.status != null && jqXHR.status == 0) {
	        			$(SUMMARY_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
	        		}
	        	}
			}
		}
	});


	return DCSServerView;
});
