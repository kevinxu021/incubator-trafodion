// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/active_query_detail.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'views/RefreshTimerView',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        ], function (BaseView, WorkloadsT, $, wHandler, moment, common, refreshTimer) {
	'use strict';
    var LOADING_SELECTOR = "#loadingImg",
    	REFRESH_INTERVAL = '#refreshInterval',
  		REFRESH_MENU = '#refreshAction',
  		QCANCEL_MENU = '#cancelAction',
  		EXPLAIN_BUTTON = '#historical-explain-btn';

    var _this = null;
    var queryID = null;
    var historySummary = {};
	var historyStatistic = {};
	var ERROR_CONTAINER = '#error-container';
	var ERROR_TEXT = '#query-detail-error-text';
	var ActiveQueryDetailView = BaseView.extend({
		template:  _.template(WorkloadsT),

		doInit: function (args){
			_this = this;
			$('#query-id').val(args);
			queryID = args;
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			refreshTimer.init();
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.setRefreshInterval(0.5);
			
			$(REFRESH_MENU).on('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(EXPLAIN_BUTTON).on('click', this.explainQuery);
			$(ERROR_CONTAINER).hide();
			this.fetchActiveQueryDetail();
			
		},
		doResume: function(args){
			$('#query-id').val(args);
			queryID = args;
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.on(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).on('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(EXPLAIN_BUTTON).on('click', this.explainQuery);
			refreshTimer.eventAgg.on(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.resume();
			$(ERROR_CONTAINER).hide();
			this.fetchActiveQueryDetail();
		},
		doPause: function(){
			refreshTimer.eventAgg.off(refreshTimer.events.TIMER_BEEPED, this.timerBeeped);
			refreshTimer.pause();
			wHandler.off(wHandler.FETCH_ACTIVE_QUERY_DETAIL_SUCCESS, this.displayResults);
			wHandler.off(wHandler.FETCH_ACTIVE_QUERY_DETAIL_ERROR, this.showErrorMessage);
			wHandler.off(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.off(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			$(REFRESH_MENU).off('click', this.fetchActiveQueryDetail);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			$(EXPLAIN_BUTTON).off('click', this.explainQuery);
		},
        showLoading: function(){
        	$(LOADING_SELECTOR).show();
        },

        hideLoading: function () {
        	$(LOADING_SELECTOR).hide();
        },
        cancelQuery: function(){
        	wHandler.cancelQuery(queryID);
        },
        cancelQuerySuccess:function(){
        	alert('The cancel query request has been submitted');
        },
        cancelQueryError:function(jqXHR){
        	alert(jqXHR.responseText);
        },
		timerBeeped: function(){
			_this.fetchActiveQueryDetail();
		},
        explainQuery: function(){
        	var queryText = $('#query-text').text();
			sessionStorage.setItem(queryID, JSON.stringify({type: 'active', text: queryText}));	
			window.location.hash = '/workloads/queryplan/'+queryID;
        },
        fetchActiveQueryDetail: function(){
			_this.showLoading();
			//$(ERROR_CONTAINER).hide();
			wHandler.fetchActiveQueryDetail(queryID);
		},

		displayResults: function (result){
			_this.hideLoading();
			$(ERROR_CONTAINER).hide();

			if (historySummary.Qid != queryID) {
				_this.clearPage();
				historyStatistic = {};
				historySummary = {};
			}

			//summary
			var summary = result.summary;
			var queryText = summary.sqlSrc;
			queryText = queryText.substring(1,queryText.length-1);
			$('#query-text').text(queryText);
			for ( var k in summary) {
				var htmlTag = "#" + k;
				var value = _this.formatSummary(k, summary[k]);
				$(htmlTag).val(value);
			}
			historySummary = result.summary;
			
			// statistic table
			var statisticDataSet = [];
			var dataSet = result.operators;
			for (var i = 0; i < dataSet.length; i++) {
				var lc = dataSet[i].LC;
				var rc = dataSet[i].RC;
				var id = dataSet[i].TDB_ID;
				var pid = dataSet[i].Parent_TDB_ID;
				var eid = dataSet[i].Explain_TDB_ID;
				var seq_num = dataSet[i].Seq_Num;
				var frag_num = dataSet[i].Frag_Num;
				var tdb_name = dataSet[i].TDB_Name;
				var est_rows = common.formatNumberWithCommas(dataSet[i].Estimated_Rows);
				var act_rows = common.formatNumberWithCommas(dataSet[i].Actual_Rows);
				var dop = dataSet[i].DOP;
				var oper_cpu_time = common.microsecondsToStringExtend(dataSet[i].Oper_CPU_Time);
				var details = dataSet[i].Details;
				
				if (!( historyStatistic[id] === undefined)) {
					if (historyStatistic[id].act_rows != act_rows) {
						act_rows = "<sflag>" + act_rows + "</eflag>";
					}
					if (historyStatistic[id].dop != dop) {
						dop = "<sflag>" + dop + "</eflag>";
					}
					if (historyStatistic[id].oper_cpu_time != oper_cpu_time) {
						oper_cpu_time = "<sflag>" + oper_cpu_time + "</eflag>";
					}
				}

				statisticDataSet.push([lc, rc, id, pid, eid,frag_num, tdb_name, dop, oper_cpu_time, est_rows, act_rows, _this.formatDetail(details,historyStatistic[id])]);
				// update history
				historyStatistic[id] = {};
				historyStatistic[id]["act_rows"] = common.formatNumberWithCommas(dataSet[i].Actual_Rows);
				historyStatistic[id]["dop"] = dataSet[i].DOP;
				historyStatistic[id]["oper_cpu_time"] = common.microsecondsToStringExtend(dataSet[i].Oper_CPU_Time);
				historyStatistic[id]["details"] = details;
			}

			var statisticTable = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="statistic-results"></table>';
			$('#statistic-container').html(statisticTable);
			$('#statistic-results').dataTable({
						"bAutoWidth" : true,
						"bProcessing" : true,
						"bFilter" : false,
						"bPaginate" : false,
						// "bAutoWidth": true,
						"scrollCollapse" : true,
						"aaData" : statisticDataSet,
						"aaSorting" :	[[2,"desc"]],
						"columns" : [{"sTitle" : "LC"},{"sTitle" : "RC"},{"sTitle" : "Id"},
								{"sTitle" : "PaId"},{"sTitle" : "ExId"},{"sTitle" : "Frag"},
								{"sTitle" : "TDB Name"},{"sTitle" : "DOP"},{"sTitle" : "Oper Cpu Time"},
								{"sTitle" : "Est. Records Used"},{"sTitle" : "Act. Records Used"},{"sTitle" : "Details"} ],
						"columnDefs" : [ {
							"targets" : [ 7, 9, 10, 11],
							"render" : function(data,type, full) {
								var data = data;
								if (!(data === undefined)) {
									data = data.toString();
									data = data.replace(/sflag/g,"span style='color:blue'")
									data = data.replace(/eflag/g,"span")
									return data;
								} 
								return "";
							}
						} ]
					});
		
		},
		
		
		formatDetail : function(currentDetails, history){
			var details ={};
			var sortKeys = [];
			var sortDetails = [];
			for ( var k in currentDetails) {
				var val = currentDetails[k];
				var flag = false;
				if(!(history === undefined)){
					if (history.details[k]  === undefined || history.details[k] != val){
						flag = true; //compare history
					}
				}
				if(val == "-1"){//format display
					val = "";
				}else{
					if(k=="CpuTime" || k=="waitTime" || k=="CumulativeReadTime" || k=="HbaseSumIOTime" || k=="OFPhaseStartTime" 
						|| k=="HbaseMaxIOTime" || k=="hdfsAccessLayerTime" || k=="hdfsConnectionTime" || k=="cursorElapsedTime" ){
						val = common.microsecondsToStringExtend(val);
					}else if(k=="bmoHeapWM" || k =="bmoHeapTotal" || k == "bmoHeapUsed" || k == "scrBuffferRead" || k == "scrBufferWritten"  
						|| k == "scrBufferBlockSize" || k == "bmoSpaceBufferSize" || k == "BytesToRead" || k == "MessagesBytes" ){
						val = common.bytesToSize(parseInt(val));
					}else{
						val = common.formatNumberWithCommas(val);
					}
				}
				if (flag == true){
					val = "<sflag>" + val + "</eflag>";
				}
				details[k] = val;
				sortKeys.push(k);
			}
			sortKeys.sort(); 
			for (var index = 0; index < sortKeys.length; index++) { //detail to string
				sortDetails.push(sortKeys[index] + ":   " + details[sortKeys[index]]);
		    }
			return sortDetails.join("</br>");
		},
		
		formatSummary : function(key, value){
			if (value == "-1") {
				value = ""
			}
			if (key == "exeElapsedTime" || key == "compElapsedTime" || key == "CanceledTime"|| key == "lastSuspendTime") {
				value = common.microsecondsToString(value);
			}
			if (key == "State"){
				var state ={
						1:"INITIAL",2:"OPEN",3:"EOF",4:"CLOSE",5:"DEALLOCATED",
						6:"FETCH",7:"CLOSE_TABLES",8:"PROCESS_ENDED",9:"UNKNOWN",10:"NULL"
				}
				value = state[value];
			}
			if (key == "StatsType"){
				var statsCollectionType = {
						0:"SQLCLI_NO_STATS",2:"SQLCLI_ACCUMULATED_STATS",3:"SQLCLI_PERTABLE_STATS",5:"SQLCLI_OPERATOR_STATS"
				}
				value = statsCollectionType[value];
			}
			if (key == "rowsReturned" || key == "RowsAffected" || key == "EstRowsAccessed"|| key == "EstRowsUsed"){
				value = common.formatNumberWithCommas(value);
			}
			return value;
		},

		clearPage: function(){
			$(".dbmgr-form-workload-cell-label").val("");
			$('#query-text').text("");
			//$('#statistic-results').DataTable().clear().draw();
			$('#statistic-results').DataTable().destroy(true);
		},
		test: function(){
        	_this.showErrorMessage();
        },
		
        showErrorMessage: function (jqXHR) {
			if(jqXHR.statusText != 'abort'){
				_this.hideLoading();
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


	return ActiveQueryDetailView;
});
