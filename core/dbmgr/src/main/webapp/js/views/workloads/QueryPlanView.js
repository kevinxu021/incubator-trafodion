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
	QCANCEL_MENU = '#cancelAction',    	
	TOOLTIP_DIALOG = '#tooltipDialog',
	TOOLTIP_DAILOG_LABEL = '#toolTipDialogLabel',
	TOOLTIP_CONTAINER = '#tooltipContainer';

	var _this = null;
	var queryID = null;
	var queryType = null;
	var setRootNode = false;
	var st = null;
	var resizeTimer = null;	
	var xhr = null;

	var QuerPlanView = BaseView.extend({
		template:  _.template(QueryPlanT),

		doInit: function (args){
			_this = this;

			this.processArgs(args);
			
			$(TOOLTIP_DIALOG).on('show.bs.modal', function () {
			       $(this).find('.modal-body').css({
			              width:'auto', //probably not needed
			              height:'auto', //probably not needed 
			              'max-height':'100%'
			       });
	        	});
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
			queryType = null;
			$('#query-text').text('');
			
			var queryParams = sessionStorage.getItem(queryID);
			sessionStorage.removeItem(queryID);
			if(queryParams != null){
				queryParams = JSON.parse(queryParams);
				queryType = queryParams.type;
				if(queryParams.text)
					$('#query-text').text(queryParams.text);
			}
		},
		fetchExplainPlan: function(){
			var queryText = $("#query-text").val();

			$("#infovis").hide();
			$("#errorText").hide();
			$(TEXT_PLAN_CONTAINER).hide();
			var param = {sQuery : queryText, sControlStmts: "", sQueryID: queryID, sQueryType: queryType};


			_this.showLoading();
			$(ERROR_CONTAINER).hide();
			//wHandler.fetchExplainPlan(param);
			if(xhr && xhr.readyState !=4){
				xhr.abort();
			}
			xhr = $.ajax({
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

			st = common.generateExplainTree(jsonData, setRootNode, _this.showExplainTooltip);
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
			if(jqXHR.statusText != 'abort'){
				_this.hideLoading();
				$(ERROR_CONTAINER).show();
				$(TEXT_PLAN_CONTAINER).hide();
				if (jqXHR.responseText) {
					$(ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
	        		if(jqXHR.status != null && jqXHR.status == 0) {
	        			$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
	        		}
	        	}
			}
		} , 
		showExplainTooltip: function(nodeName, data){
        	$(TOOLTIP_DIALOG).modal('show');
			nodeName = nodeName.replace("_", " ");
			nodeName = nodeName.replace("SEABASE","TRAFODION");
			nodeName = common.toProperCase(nodeName);        	
        	$(TOOLTIP_DAILOG_LABEL).text(nodeName);
        	$(TOOLTIP_CONTAINER).text(data);
        },
        toProperCase: function (s) {
        	return s.toLowerCase().replace(/^(.)|\s(.)/g, function($1) {
        		return $1.toUpperCase();
        	});
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

	});


	return QuerPlanView;
});
