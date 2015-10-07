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
        'tabletools'
        ], function (BaseView, DcsServerT, $, serverHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = ".dbmgr-spinner",
	RESULT_CONTAINER = '#query-result-container',
	ERROR_CONTAINER = '#errorText';

	var oDataTable = null;
	var _that = null;

	var DCSServerView = BaseView.extend({
		template:  _.template(DcsServerT),

		doInit: function (){
			_that = this;
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchDcsServers);
			this.fetchDcsServers();
		},
		doResume: function(){
			serverHandler.on(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchDcsServers);
			this.fetchDcsServers();
		},
		doPause: function(){
			serverHandler.off(serverHandler.FETCHDCS_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.FETCHDCS_ERROR, this.showErrorMessage);			
			$("#refreshAction").off('click', this.fetchDcsServers);
		},
		showLoading: function(){
			$('#loadingImg').show();
		},

		hideLoading: function () {
			$('#loadingImg').hide();
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
						"aTargets": [ 2 ],
						"mData": 2,
						"mRender": function ( data, type, full ) {
							//if (type === 'display') {
							return moment(data).tz(common.serverTimeZone).format('YYYY-MM-DD HH:mm:ss');
							// }
							/// else return moment(data).tz(common.serverTimeZone).format('YYYY-MM-DD HH:mm:ss');
						}
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


	return DCSServerView;
});
