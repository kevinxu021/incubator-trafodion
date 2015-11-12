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

			if (historySummary.Qid != queryID) {
				// different workload, clear history
				historyStatistic = {};
				historySummary = {};
			}

			// summary
			var summary = result.summary;
			var queryText = summary.sqlSrc;
			queryText = queryText.substring(1,queryText.length-1);
			$('#query-text').text(queryText);
			//sessionStorage.setItem(queryID, queryText);	

			for ( var k in summary) {
				var htmlTag = "#" + k;
				var value = summary[k];
				if (historySummary[k] != undefined && historySummary[k] != value) {
					$(htmlTag).css("color", "blue");
				} else {
					$(htmlTag).css("color", "black");
				}
				if (value == "-1") {
					value = ""
				}
				if (k == "exeElapsedTime" || k == "compElapsedTime" || k == "CanceledTime"|| k == "lastSuspendTime") {
					value = common.microsecondsToString(value);
				}
				if (k == "State"){
					var state ={
							1:"INITIAL",2:"OPEN",3:"EOF",4:"CLOSE",5:"DEALLOCATED",
							6:"FETCH",7:"CLOSE_TABLES",8:"PROCESS_ENDED",9:"UNKNOWN",10:"NULL"
					}
					value = state[value];
				}
				if (k == "StatsType"){
					var statsCollectionType = {
							0:"SQLCLI_NO_STATS",2:"SQLCLI_ACCUMULATED_STATS",3:"SQLCLI_PERTABLE_STATS",5:"SQLCLI_OPERATOR_STATS"
					}
					value = statsCollectionType[value];
				}
				$(htmlTag).val(value);
			}
			historySummary = result.summary
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
				var details = _this.formatDetail(dataSet[i].Details);
				
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
					details = _this.flagDetail(historyStatistic[id].details,details);
				}

				statisticDataSet.push([lc, rc, id, pid, eid,frag_num, tdb_name, dop, oper_cpu_time, est_rows, act_rows, _this.detailToString(details)]);
				// update history
				historyStatistic[id] = {};
				historyStatistic[id]["act_rows"] = common.formatNumberWithCommas(dataSet[i].Actual_Rows);
				historyStatistic[id]["dop"] = dataSet[i].DOP;
				historyStatistic[id]["oper_cpu_time"] = common.microsecondsToStringExtend(dataSet[i].Oper_CPU_Time);
				historyStatistic[id]["details"] = _this.formatDetail(dataSet[i].Details);
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
							"targets" : [ 7, 9, 10],
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
		
		flagDetail : function(history, latest) {
			for ( var k in latest) {
				if(history[k] === undefined || history[k] != latest[k]){
					latest[k] = "<sflag>" + latest[k] + "</eflag>";
				}else{
					latest[k] =  latest[k];
				}
			}
			return latest;
		},

		detailToString : function(detail){
			var arr = [];
			for (var k in detail){
				arr.push(k + ":   " + detail[k])
			}
			return arr.join("</br>");
		},
		
		formatDetail : function(Obj) {
			var result = {};
			for ( var o in Obj) {
				var key = o;
				var value = Obj[o];
				if(value == "-1"){
					value = "";
				}else{
					if(key=="CpuTime" || key=="waitTime" || key=="CumulativeReadTime" || key=="HbaseSumIOTime" || key=="OFPhaseStartTime" 
						|| key=="HbaseMaxIOTime" || key=="hdfsAccessLayerTime" || key=="hdfsConnectionTime" || key=="cursorElapsedTime" ){
						value = common.microsecondsToStringExtend(value);
					}else if(key=="bmoHeapWM" || key =="bmoHeapTotal" || key == "bmoHeapUsed" || key == "scrBuffferRead" || key == "scrBufferWritten"  
						|| key == "scrBufferBlockSize" || key == "bmoSpaceBufferSize" || key == "BytesToRead" || key == "MessagesBytes" ){
						value = common.bytesToSize(parseInt(value));
					}else{
						value = common.formatNumberWithCommas(value);
					}
				}
				result[key] = value;
			}
			return result;
		},

        showErrorMessage: function (jqXHR) {
        	_this.hideLoading();
        	/*$(RESULT_CONTAINER).hide();
        	$(ERROR_CONTAINER).show();
        	if (jqXHR.responseText) {
        		$(ERROR_CONTAINER).text(jqXHR.responseText);
        	}*/
        }  

	});


	return ActiveQueryDetailView;
});
