//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/query_plan.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'moment',
        'common',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'datetimepicker',
        'jqueryvalidate'
        ], function (BaseView, QueryPlanT, $, wHandler, moment, common) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	ERROR_CONTAINER = '#visual-plan-error',
	TEXT_PLAN_CONTAINER = '#text-result-container',
	GRIDCONTAINER = "#dbmgr-1",
	REFRESH_MENU = '#refreshAction',
	QCANCEL_MENU = '#cancelAction';

	var _this = null;
	var queryID = null;
	var setRootNode = false;
	var st = null;
	var resizeTimer = null;	

	var QuerPlanView = BaseView.extend({
		template:  _.template(QueryPlanT),

		doInit: function (args){
			_this = this;

			this.processArgs(args);

			$(REFRESH_MENU).on('click', this.fetchExplainPlan());
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			
			this.fetchExplainPlan();

		},
		doResume: function(args){
			this.processArgs(args);

			$(REFRESH_MENU).on('click', this.fetchExplainPlan);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			this.fetchExplainPlan();
		},
		doPause: function(){
			$(REFRESH_MENU).off('click', this.fetchExplainPlan);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			wHandler.off(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.off(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		onRelayout: function () {
			this.onResize();
		},
		onResize: function () {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(this.doResize, 200);
		},
		doResize: function () {
			if(st != null) {
				st.canvas.resize($('#infovis').width(), ($(GRIDCONTAINER).height() + $(GRIDCONTAINER).scrollTop() + 800));
			}
		},
		processArgs: function(args){
			$('#query-id').val(args);
			queryID = args;

			var queryText = sessionStorage.getItem(queryID);
			sessionStorage.removeItem(queryID);
			if(queryText != null){
				$('#query-text').text(queryText);
			}
		},
		fetchExplainPlan: function(){
			var queryText = $("#query-text").val();

			$("#infovis").hide();
			$("#errorText").hide();
			$(TEXT_PLAN_CONTAINER).hide();
			var param = {sQuery : queryText, sControlStmts: "", sQueryID: queryID};


			_this.showLoading();
			$(ERROR_CONTAINER).hide();
			//wHandler.fetchExplainPlan(param);
			$.ajax({
				url:'resources/queries/explain',
				type:'POST',
				data: JSON.stringify(param),
				dataType:"json",
				contentType: "application/json;",
				success:_this.drawExplain,
				error:function(jqXHR, res, error){
					_this.hideLoading();
					_this.showErrorMessage(jqXHR);
				}
			});
		},

		drawExplain: function (jsonData){
			_this.hideLoading();
			$('#text-result-container').show();
			$('#text-result').text(jsonData.planText);
			$("#infovis").empty();

			st = common.generateExplainTree(jsonData, setRootNode);
			//load json data
			st.loadJSON(jsonData);

			$(TEXT_PLAN_CONTAINER).show();

			//compute node positions and layout
			st.compute();
			$("#infovis").show();
			//emulate a click on the root node.
			st.onClick(st.root);
			_this.doResize();			
		},

		showErrorMessage: function (jqXHR) {
			_this.hideLoading();
			$(ERROR_CONTAINER).show();
			$(TEXT_PLAN_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}
		} , 
		cancelQuery: function(){
			wHandler.cancelQuery(queryID);
		},
		cancelQuerySuccess:function(){
			alert('The cancel query request has been submitted');
		},
		cancelQueryError:function(jqXHR){
			alert(jqXHR.responseText);
		},        

	});


	return QuerPlanView;
});
