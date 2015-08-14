define([
        'views/BaseView',
        'text!templates/logs.html',
        'jquery',
        'handlers/LogsHandler',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools'
        ], function (BaseView, LogsT, $, logsHandler) {
	'use strict';
    var LOADING_SELECTOR = ".dbmgr-spinner",
    	RESULT_CONTAINER = '#query-result-container',
    	ERROR_CONTAINER = '#errorText';
    
    var oDataTable = null;
    var _that = null;

	var LogsView = BaseView.extend({
		template:  _.template(LogsT),

		init: function (){
			_that = this;
			logsHandler.on(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.on(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchLogs);
			this.fetchLogs();
		},
		resume: function(){
			logsHandler.on(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.on(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$("#refreshAction").on('click', this.fetchLogs);
			this.fetchLogs();
		},
		pause: function(){
			logsHandler.off(logsHandler.FETCHLOGS_SUCCESS, this.displayResults);
			logsHandler.off(logsHandler.FETCHLOGS_ERROR, this.showErrorMessage);			
			$("#refreshAction").off('click', this.fetchLogs);
		},
        showLoading: function(){
        	$('#loadingImg').show();
        },

        hideLoading: function () {
        	$('#loadingImg').hide();
        },
        fetchLogs: function () {
			_that.showLoading();
			$(ERROR_CONTAINER).hide();
			logsHandler.fetchLogs();
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
					"bAutoWidth": true,
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					paging: true,
					"tableTools": {
						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					},
					aaSorting: [[ 0, "desc" ]],
					fnDrawCallback: function(){
						$('#query-results td').css("white-space","nowrap");
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


	return LogsView;
});
