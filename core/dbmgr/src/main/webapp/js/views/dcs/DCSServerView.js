//@@@ START COPYRIGHT @@@
//
//(C) Copyright 2015 Esgyn Corporation
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
        'datatables',
        'datatablesBootStrap',
        'tablebuttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, DcsServerT, $, serverHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg',
	RESULT_CONTAINER = '#dcs-result-container',
	ERROR_CONTAINER = '#dcs-error-text',
	REFRESH_ACTION = '#refreshAction';

	var oDataTable = null;
	var _that = null;

	var DCSServerView = BaseView.extend({
		template:  _.template(DcsServerT),

		doInit: function (){
			_that = this;
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).on('click', this.fetchDcsServers);
			this.fetchDcsServers();
		},
		doResume: function(){
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).on('click', this.fetchDcsServers);
			this.fetchDcsServers();
		},
		doPause: function(){
			serverHandler.off(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$(REFRESH_ACTION).off('click', this.fetchDcsServers);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		fetchDcsServers: function () {
			_that.showLoading();
			$(ERROR_CONTAINER).hide();
			serverHandler.fetchDcsServers();
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

				oDataTable = $('#dcs-query-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no dcs servers"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					//autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
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
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Connectivity Servers' }
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
		}  
	});


	return DCSServerView;
});
