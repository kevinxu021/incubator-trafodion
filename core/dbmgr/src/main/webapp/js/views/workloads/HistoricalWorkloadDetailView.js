//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/history_workload_detail.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datetimepicker',
        'jqueryvalidate',
        'bootstrapNotify'
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common, CodeMirror) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	REFRESH_MENU = '#refreshAction',
	QCANCEL_MENU = '#cancelAction',
	EXPLAIN_BUTTON = '#historical-explain-btn',
	DETAILS_CLASS = '.dbmgr-query-detailsr',
	RESULT_CONTAINER = '#details-container',
	ERROR_CONTAINER = '#error-container',
	ERROR_TEXT = '#query-detail-error-text';
	
	var CANCEL_QUERY_DIALOG = '#cancel-query-dialog',
		CANCEL_QUERY_ID = '#cancel-query-id',
		CANCEL_QUERY_YES_BTN = '#cancel-query-yes-btn';
	
	var _this = null;
	var queryID = null;
	var connDataTable = null, compDataTable = null, runDataTable = null;
	var queryTextEditor = null;
	
	var HistoricalWorkloadDetailView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (args){
			this.redirectFlag=false;
			_this = this;
			this.pageIdentifier="historical";
			$('#query-id').val(args);
			queryID = args;
			this.currentURL = window.location.hash;
			if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
				common.defineEsgynSQLMime(CodeMirror);
			}

			queryTextEditor = CodeMirror.fromTextArea(document.getElementById("query-text"), {
				mode: 'text/x-esgyndb',
				indentWithTabs: false,
				smartIndent: false,
				lineNumbers: false,
				lineWrapping: true,
				matchBrackets : true,
				readOnly: true,
				autofocus: true,
				extraKeys: {"Ctrl-Space": "autocomplete"}
			});
			$(queryTextEditor.getWrapperElement()).resizable({
				resize: function() {
					queryTextEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(queryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "width":"800px", "height":"150px"});
			
			this.loadQueryText();
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).on('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			$(EXPLAIN_BUTTON).on('click', this.explainQuery);
			$(ERROR_CONTAINER).hide();
			this.fetchRepositoryQueryDetail();

		},
		doResume: function(args){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			$('#query-id').val(args);
			if(queryID != null && queryID != args){
				queryID = args;
				$(RESULT_CONTAINER).show();
				$(DETAILS_CLASS).show();
				$(ERROR_CONTAINER).hide();
				$('#query-start-time').val('');
				$('#query-end-time').val('');
				//$('#query-text').text('');
				queryTextEditor.setValue('');
				this.loadQueryText();
				$('#query-status').val('');
				try{
					if(connDataTable != null){
						connDataTable.fnClearTable();
					}
				}catch(Error){

				}
				try{
					if(compDataTable != null){
						compDataTable.fnClearTable();
					}
				}catch(Error){

				}
				try{
					if(runDataTable != null){
						runDataTable.fnClearTable();
					}
				}catch(Error){

				}
			}
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			$(REFRESH_MENU).on('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			$(EXPLAIN_BUTTON).on('click', this.explainQuery);
			this.fetchRepositoryQueryDetail();
		},
		doPause: function(){
			this.redirectFlag=true;
			wHandler.off(wHandler.FETCH_REPO_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_REPO_QUERY_DETAIL_ERROR, this.showErrorMessage);
			$(REFRESH_MENU).off('click', this.fetchRepositoryQueryDetail);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			$(CANCEL_QUERY_YES_BTN).on('click', this.cancelQueryConfirmed);
			$(EXPLAIN_BUTTON).off('click', this.explainQuery);
		},
		loadQueryText: function(){
			var queryParams = sessionStorage.getItem(queryID);
			sessionStorage.removeItem(queryID);
			if(queryParams != null){
				queryParams = JSON.parse(queryParams);
				if(queryParams.text && queryTextEditor)
					queryTextEditor.setValue(queryParams.text);
			}
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
				$(CANCEL_QUERY_ID).text(queryID);
				$(CANCEL_QUERY_DIALOG).modal('show');
			}else {
				/*alert("The query is not in executing state. Cannot cancel the query.");*/
				var msgObj={msg:'The query '+queryID+' is not in executing state. Cannot cancel the query.',tag:"warning",url:_this.currentURL,shortMsg:"The query is not in executing state."};
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
			}
		},
		cancelQueryConfirmed: function(){
			wHandler.cancelQuery(queryID, _this);			
		},
		cancelQuerySuccess:function(data){
			if(data.requestor == _this){
				var msgObj={msg:'The cancel query request has been submitted',tag:"success",url:_this.currentURL,shortMsg:"Cancel query successfully."};
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
				_this.fetchRepositoryQueryDetail();
			}
		},
		cancelQueryError:function(jqXHR){
			if(jqXHR.requestor == _this){
				var msgObj={msg:jqXHR.responseText,tag:"danger",url:_this.currentURL,shortMsg:"Cancel query failed."};
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
				_this.fetchRepositoryQueryDetail();
			}
		},  
		explainQuery: function(){
			var queryText = queryTextEditor.getValue(); //$('#query-text').text();
			sessionStorage.setItem(queryID, JSON.stringify({type: 'repo', text: queryText}));	
			window.location.hash = '/workloads/queryplan/'+encodeURIComponent(queryID);
		},
		fetchRepositoryQueryDetail: function(){
			_this.showLoading();
			//$(ERROR_CONTAINER).hide();
			wHandler.fetchRepositoryQueryDetail(queryID);
		},

		displayResults: function (result){
			_this.hideLoading();
			$(RESULT_CONTAINER).show();
			$(DETAILS_CLASS).show();
			$(ERROR_CONTAINER).hide();

			//$('#query-text').text(result.queryText);
			queryTextEditor.setValue(result.queryText);
	
			$('#query-status').val(result.status.trim());
			var startTimeVal = "";
			if(result.startTime != null && result.startTime != -1){
				startTimeVal = common.toServerLocalDateFromUtcMilliSeconds(result.startTime);
			}
			var endTimeVal = "";
			if(result.endTime != null && result.endTime != -1){
				endTimeVal = common.toServerLocalDateFromUtcMilliSeconds(result.endTime);
			}			
			var lastUpdTimeVal = "";
			if(result.metrics.last_updated_time != null && result.metrics.last_updated_time != -1){
				lastUpdTimeVal = common.toServerLocalDateFromUtcMilliSeconds(result.metrics.last_updated_time);
			}
			$('#query-start-time').val(startTimeVal);
			$('#query-end-time').val(endTimeVal);
			$('#last-update-time').val(lastUpdTimeVal);
			
			var colNames = [{"title":"Name"}, {"title":"Value"}];

			var connDataSet = [];
			connDataSet.push(["User", result.userName]);
			connDataSet.push(["Application", result.metrics.application_name]);
			connDataSet.push(["Client Name", result.metrics.client_name]);
			connDataSet.push(["Session ID", result.metrics.session_id]);
			if(common.enableWMS){
				connDataSet.push(["Profile Name", result.metrics.profile_name]);
				connDataSet.push(["SLA Name", result.metrics.sla_name]);
			}
			connDataSet.push(["Transaction ID", result.metrics.transaction_id]);
			connDataSet.push(["Statement Type", result.metrics.statement_type]);
			connDataSet.push(["Statement Sub Type", result.metrics.statement_subtype]);
			connDataSet.push(["Process Name", result.metrics.process_name]);
			connDataSet.push(["Master Process ID", result.metrics.master_process_id]);
			connDataSet.push(["Submit Time", common.toServerLocalDateFromUtcMilliSeconds(result.metrics.submit_utc_ts)]);
			connDataSet.push(["Node Count", common.formatNumberWithCommas(result.metrics.num_nodes)]);

			var compDataSet = [];
			compDataSet.push(["Compile Start Time", common.toServerLocalDateFromUtcMilliSeconds(result.metrics.compile_start_utc_ts)]);
			compDataSet.push(["Compile End Time", common.toServerLocalDateFromUtcMilliSeconds(result.metrics.compile_end_utc_ts)]);
			compDataSet.push(["Compile Elapsed Time", common.microsecondsToStringExtend(result.metrics.compile_elapsed_time)]);
			compDataSet.push(["Degree of Parallelism", result.metrics.cmp_dop]);
			compDataSet.push(["Number of Joins", result.metrics.cmp_num_joins]);
			compDataSet.push(["Full Scan", common.formatTrueOrFalse(result.metrics.cmp_full_scan_on_table)]);
			compDataSet.push(["Est.Accessed Rows", common.formatNumberWithCommas(result.metrics.est_accessed_rows)]);
			compDataSet.push(["Est.Used Rows", common.formatNumberWithCommas(result.metrics.est_used_rows)]);
			compDataSet.push(["Est.Cost", result.metrics.est_cost]);
			compDataSet.push(["Est.Cardinality", common.formatNumberWithCommas(result.metrics.est_cardinality)]);
			compDataSet.push(["Est.Memory Use", common.bytesToSize(result.metrics.est_total_mem*1024)]);
			compDataSet.push(["Est.CPU Time", common.millisecondsToString(result.metrics.est_cpu_time/1000)]);
			compDataSet.push(["Est.IO Time", common.millisecondsToString(result.metrics.est_io_time/1000)]);
			compDataSet.push(["Est.Message Time", common.millisecondsToString(result.metrics.est_msg_time/1000)]);
			compDataSet.push(["Est.Idle Time", common.millisecondsToString(result.metrics.est_idle_time/1000)]);
			compDataSet.push(["CPU Path Length", common.microsecondsToStringExtend(result.metrics.cmp_cpu_path_length)]);
			compDataSet.push(["Number of BMOs", result.metrics.cmp_number_of_bmos]);
			compDataSet.push(["Compile Overflow Size", common.bytesToSize(result.metrics.cmp_overflow_size*1024)]);

			var runtimeDataSet = [];
			runtimeDataSet.push(["Query Elapsed Time", common.microsecondsToStringExtend(result.metrics.query_elapsed_time)]);
			connDataSet.push(["Process Count", result.metrics.processes_created]);
			runtimeDataSet.push(["Process Create Busy Time", common.microsecondsToStringExtend(result.metrics.process_create_busy_time)]);
			runtimeDataSet.push(["SQL Process Busy Time", common.microsecondsToStringExtend(result.metrics.sql_process_busy_time)]);
			runtimeDataSet.push(["Disk Process Busy Time", common.microsecondsToStringExtend(result.metrics.disk_process_busy_time)]);
			runtimeDataSet.push(["UDR Process Busy Time", common.microsecondsToStringExtend(result.metrics.udr_process_busy_time)]);
			runtimeDataSet.push(["Master Execution Time", common.microsecondsToStringExtend(result.metrics.master_execution_time)]);
			runtimeDataSet.push(["Disk IOs", common.formatNumberWithCommas(result.metrics.disk_ios)]);
			runtimeDataSet.push(["SQL Process Count", result.metrics.num_sql_processes]);
			runtimeDataSet.push(["Total Memory Allocated", common.bytesToSize(result.metrics.total_mem_alloc*1024)]);
			//runtimeDataSet.push(["Max. Memory Used", common.bytesToSize(result.metrics.max_mem_used*1024)]);
			runtimeDataSet.push(["Error Code", result.metrics.error_code]);
			runtimeDataSet.push(["Stats Error Code", result.metrics.stats_error_code]);
			runtimeDataSet.push(["SQL Error Code", result.metrics.sql_error_code]);
			runtimeDataSet.push(["Error Text", result.metrics.error_text]);
			runtimeDataSet.push(["AQR Retry Count", result.metrics.total_num_aqr_retries]);
			runtimeDataSet.push(["IUD Row Count", result.metrics.num_rows_iud >=0 ? common.formatNumberWithCommas(result.metrics.num_rows_iud) : 0]);
			runtimeDataSet.push(["Messages To Disk", result.metrics.msgs_to_disk]);
			runtimeDataSet.push(["Message Size to Disk", common.bytesToSize(result.metrics.msg_bytes_to_disk)]);
			runtimeDataSet.push(["Overflow Size Written", common.bytesToSize(result.metrics.ovf_buffer_bytes_written*1024)]);
			runtimeDataSet.push(["Overflow Size Read", common.bytesToSize(result.metrics.ovf_buffer_bytes_read*1024)]);


			var connTable = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="conn-metrics-results"></table>';
			$('#connection-metrics-container').html( connTable );

			connDataTable = $('#conn-metrics-results').dataTable({
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

			compDataTable = $('#compile-metrics-results').dataTable({
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

			runDataTable = $('#run-metrics-results').dataTable({
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
			if(jqXHR.statusText != 'abort'){
				_this.hideLoading();
				$(RESULT_CONTAINER).hide();
				$(DETAILS_CLASS).hide();
				$(ERROR_CONTAINER).show();
				$(ERROR_TEXT).text("");
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


	return HistoricalWorkloadDetailView;
});
