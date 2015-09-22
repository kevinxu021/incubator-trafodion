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
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools'
        ], function (BaseView, WorkloadsT, $, wHandler, common) {
	'use strict';
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var oDataTable = null;
    var _that = null;

	var ActiveWorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		init: function (){
			_that = this;
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);
			$("#refreshAction").on('click', this.fetchActiveQueries);
			this.fetchActiveQueries();
		},
		resume: function(){
			wHandler.on(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchActiveQueries);
			this.fetchActiveQueries();
		},
		pause: function(){
			wHandler.off(wHandler.FETCH_ACTIVE_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_ACTIVE_ERROR, this.showErrorMessage);			
			$("#refreshAction").off('click', this.fetchActiveQueries);
		},
        showLoading: function(){
        	$('#loadingImg').show();
        },

        hideLoading: function () {
        	$('#loadingImg').hide();
        },
        fetchActiveQueries: function () {
			_that.showLoading();
			wHandler.fetchActiveQueries();
		},

		displayResults: function (result){
			_that.hideLoading();
			$("#errorText").hide();
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
        	_that.hideLoading();
        	$("#errorText").show();
        	if (jqXHR.responseText) {
        		$("#errorText").text(jqXHR.responseText);
        	}
        }  
	});


	return ActiveWorkloadsView;
});
