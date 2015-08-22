define([
        'views/BaseView',
        'text!templates/workloads.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools'
        ], function (BaseView, WorkloadsT, $, wHandler) {
	'use strict';
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var oDataTable = null;
    var _that = null;

	var WorkloadsView = BaseView.extend({
		template:  _.template(WorkloadsT),

		init: function (){
			_that = this;
			wHandler.on(wHandler.FETCHWORKLOADS_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCHWORKLOADS_ERROR, this.showErrorMessage);
			$("#refreshAction").on('click', this.fetchWorkloads);
			this.fetchWorkloads();
		},
		resume: function(){
			wHandler.on(wHandler.FETCHWORKLOADS_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCHWORKLOADS_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchWorkloads);
			this.fetchWorkloads();
		},
		pause: function(){
			wHandler.off(wHandler.FETCHWORKLOADS_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCHWORKLOADS_ERROR, this.showErrorMessage);			
			$("#refreshAction").off('click', this.fetchWorkloads);
		},
        showLoading: function(){
        	$('#loadingImg').show();
        },

        hideLoading: function () {
        	$('#loadingImg').hide();
        },
        fetchWorkloads: function () {
			_that.showLoading();
			wHandler.fetchWorkloads();
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
					paging: true,
					"tableTools": {
						"sRowSelect": "multi",
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},
					fnDrawCallback: function(){
						$('#query-results td').css("white-space","nowrap");
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


	return WorkloadsView;
});
