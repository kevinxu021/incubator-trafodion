define([
        'views/BaseView',
        'text!templates/history_workload_detail.html',
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
  		REFRESH_MENU = '#refreshAction',
  		QCANCEL_MENU = '#cancelAction';

    var _that = null;
    var queryID = null;
	var HistoricalWorkloadDetailView = BaseView.extend({
		template:  _.template(WorkloadsT),

		init: function (args){
			_that = this;
			$('#query-id').val(args);
			queryID = args;
			$('#explainLink').attr("href", "#/workloads/history/queryplan/"+queryID);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).on('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			this.fetchRepositoryQueryDetail();
			
		},
		resume: function(args){
			$('#query-id').val(args);
			queryID = args;
			$('#explainLink').attr("href", "#/workloads/history/queryplan/"+queryID);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).on('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			this.fetchRepositoryQueryDetail();
		},
		pause: function(){
			wHandler.off(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.off(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.off(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).off('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
		},
        showLoading: function(){
        	$(LOADING_SELECTOR).show();
        },

        hideLoading: function () {
        	$(LOADING_SELECTOR).hide();
        },
        cancelQuery: function(){
        	var queryStatus = $('#query-status').val();
        	if(queryStatus == 'EXECUTING'){
        		wHandler.cancelQuery(queryID);
        	}else {
        		alert("The query is not in executing state. Cannot cancel the query.");
        	}
        },
        cancelQuerySuccess:function(){
        	alert('The cancel query request has been submitted');
        },
        cancelQueryError:function(jqXHR){
        	alert(jqXHR.responseText);
        },        
        fetchRepositoryQueryDetail: function(){
			_that.showLoading();
			//$(ERROR_CONTAINER).hide();
			wHandler.fetchRepositoryQueryDetail(queryID);
		},

		displayResults: function (result){
			_that.hideLoading();
			$('#query-text').text(result.queryText);
			sessionStorage.setItem(queryID, result.queryText);	
			
			$('#query-status').val(result.status);
			$('#query-start-time').val(common.toDateFromMilliSeconds(result.startTime));
			$('#query-end-time').val(common.toDateFromMilliSeconds(result.endTime));
			var colNames = [{"title":"Name"}, {"title":"Value"}];
			
			var connDataSet = [];
			connDataSet.push(["User", result.metrics.user_name]);
			connDataSet.push(["Application", result.metrics.application_name]);
			connDataSet.push(["Client Name", result.metrics.client_name]);
			connDataSet.push(["Session ID", result.metrics.session_id]);
			connDataSet.push(["Transaction ID", result.metrics.transaction_id]);
			connDataSet.push(["Statement Type", result.metrics.statement_type]);
			connDataSet.push(["Statement Sub Type", result.metrics.statement_subtype]);
			connDataSet.push(["Process Name", result.metrics.process_name]);
			connDataSet.push(["Master Process ID", result.metrics.master_process_id]);
			connDataSet.push(["Submit Time", common.toDateFromMilliSeconds(result.metrics.submit_utc_ts)]);
			connDataSet.push(["Node Count", result.metrics.num_nodes]);
			connDataSet.push(["Process Count", result.metrics.processes_created]);
			
			var compDataSet = [];
			compDataSet.push(["Compile Start Time", common.toDateFromMilliSeconds(result.metrics.compile_start_utc_ts)]);
			compDataSet.push(["Compile End Time", common.toDateFromMilliSeconds(result.metrics.compile_end_utc_ts)]);
			compDataSet.push(["Compile Elapsed Time", result.metrics.compile_elapsed_time]);
			compDataSet.push(["Degree of Parallelism", result.metrics.cmp_dop]);
			compDataSet.push(["Number of Joins", result.metrics.cmp_num_joins]);
			compDataSet.push(["Full Scan", result.metrics.cmp_full_scan_on_table]);
			compDataSet.push(["Est.Accessed Rows", result.metrics.est_accessed_rows]);
			compDataSet.push(["Est.Used Rows", result.metrics.est_used_rows]);
			compDataSet.push(["Est.Cost", result.metrics.est_cost]);
			compDataSet.push(["Est.Cardinality", result.metrics.est_cardinality]);
			compDataSet.push(["Est.Memory Use", result.metrics.est_total_mem]);
			compDataSet.push(["Est.CPU Time", result.metrics.est_cpu_time]);
			compDataSet.push(["Est.IO Time", result.metrics.est_io_time]);
			compDataSet.push(["Est.Message Time", result.metrics.est_msg_time]);
			compDataSet.push(["Est.Idle Time", result.metrics.est_idle_time]);
			compDataSet.push(["CPU Path Length", result.metrics.cmp_cpu_path_length]);
			compDataSet.push(["Number of BMOs", result.metrics.cmp_number_of_bmos]);
			compDataSet.push(["Overflow Size", result.metrics.cmp_overflow_size]);

			var runtimeDataSet = [];
			runtimeDataSet.push(["Query Elapsed Time", result.metrics.query_elapsed_time]);
			runtimeDataSet.push(["SQL Process Busy Time", result.metrics.sql_process_busy_time]);
			runtimeDataSet.push(["Disk Process Busy Time", result.metrics.disk_process_busy_time]);
			runtimeDataSet.push(["Master Execution Time", result.metrics.master_execution_time]);
			runtimeDataSet.push(["Disk IOs", result.metrics.disk_ios]);
			runtimeDataSet.push(["SQL Process Count", result.metrics.num_sql_processes]);
			runtimeDataSet.push(["Total Memory Allocated", result.metrics.total_mem_alloc]);
			runtimeDataSet.push(["Total Memory Used", result.metrics.max_mem_used]);
			runtimeDataSet.push(["Error Code", result.metrics.error_code]);
			runtimeDataSet.push(["Stats Error Code", result.metrics.stats_error_code]);
			runtimeDataSet.push(["SQL Error Code", result.metrics.sql_error_code]);
			runtimeDataSet.push(["Error Text", result.metrics.error_text]);
			runtimeDataSet.push(["AQR Retry Count", result.metrics.total_num_aqr_retries]);
			runtimeDataSet.push(["IUD Row Count", result.metrics.num_rows_iud]);
			runtimeDataSet.push(["Messages To Disk", result.metrics.msgs_to_disk]);
			runtimeDataSet.push(["Message Bytes to Disk", result.metrics.msg_bytes_to_disk]);
			
	
			var connTable = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="conn-metrics-results"></table>';
			$('#connection-metrics-container').html( connTable );

			$('#conn-metrics-results').dataTable({
				"bProcessing": true,
				"bFilter": false,
				"bPaginate" : false, 
				//"bAutoWidth": true,
				"scrollCollapse": true,
				"aaData": connDataSet, 
				"aoColumns" : colNames,
				"aoColumnDefs": [{
					  "sWidth": "50%",
				      "aTargets": [ 0 ],
				      "mData": 0,
				      "mRender": function ( data, type, full ) {
				       if (type === 'display') {
				          return data != null ? data : "";
				        }
				        else return data != null ? data : "";
				      }
				    },
				    {
				      "aTargets": [ 1 ],
				      "mData": 1,
				      "mRender": function ( data, type, full ) {
				       if (type === 'display') {
				          return data != null ? data : "";
				        }
				        else return data != null ? data : "";
				      }
				    } ],
		        aaSorting: [],
	             fnDrawCallback: function(){
						//$('#conn-metrics-results td').css("white-space","nowrap");
		             }

			});	

			var compTable = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="compile-metrics-results"></table>';
			$('#compile-metrics-container').html( compTable );

			$('#compile-metrics-results').dataTable({
				"bProcessing": true,
				"bFilter": false,
				"bPaginate" : false, 
				"bAutoWidth": true,
				"scrollCollapse": true,
				"aaData": compDataSet, 
				"aoColumns" : colNames,
				"aoColumnDefs": [ {
				      "aTargets": [ 1 ],
				      "mData": 1,
				      "mRender": function ( data, type, full ) {
				       if (type === 'display') {
				          return data != null ? data : "";
				        }
				        else return data != null ? data : "";
				      }
				    } ],
		        aaSorting: [],
	             fnDrawCallback: function(){
						//$('#conn-metrics-results td').css("white-space","nowrap");
		             }

			});	

			var runTable = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="run-metrics-results"></table>';
			$('#runtime-metrics-container').html( runTable );

			$('#run-metrics-results').dataTable({
				"bFilter": false,
				"bProcessing": true,
				"bPaginate" : false, 
				"bAutoWidth": true,
				"scrollCollapse": true,
				"aaData": runtimeDataSet, 
				"aoColumns" : colNames,
				"aoColumnDefs": [ {
				      "aTargets": [ 1 ],
				      "mData": 1,
				      "mRender": function ( data, type, full ) {
				       if (type === 'display') {
				          return data != null ? data : "";
				        }
				        else return data != null ? data : "";
				      }
				    } ],
		        aaSorting: [],
	             fnDrawCallback: function(){
						//$('#conn-metrics-results td').css("white-space","nowrap");
		             }

			});	
			//$('#conn-metrics-results td').css("white-space","nowrap");
			
		},

        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	/*$(RESULT_CONTAINER).hide();
        	$(ERROR_CONTAINER).show();
        	if (jqXHR.responseText) {
        		$(ERROR_CONTAINER).text(jqXHR.responseText);
        	}*/
        }  

	});


	return HistoricalWorkloadDetailView;
});
