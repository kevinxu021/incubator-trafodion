//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/query_plan.html',
        'jquery',
        'handlers/WorkloadsHandler',
        'handlers/ServerHandler',
        'moment',
        'common',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'datetimepicker',
        'jqueryvalidate'
        ], function (BaseView, QueryPlanT, $, wHandler, serverHandler, moment, common, CodeMirror) {
	'use strict';
	var LOADING_SELECTOR = "#loadingImg",
	ERROR_CONTAINER = '#visual-plan-error',
	TEXT_PLAN_CONTAINER = '#text-result-container',
	GRAPH_CONTAINER = '#graph-container',
	USED_TABLES_CONTAINER = '#tables-used-container',
	USED_TABLES_DIV = '#used-tables-div',
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
	var queryTextEditor = null;
	
	var QuerPlanView = BaseView.extend({
		template:  _.template(QueryPlanT),

		doInit: function (args){
			this.currentURL = window.location.hash;
			common.redirectFlag=false;
			_this = this;

			$('.panel-heading span.dbmgr-collapsible').on("click", function (e) {
		        if ($(this).hasClass('panel-collapsed')) {
		                // expand the panel
		                $(this).parents('.panel').find('.panel-body').slideDown();
		                $(this).removeClass('panel-collapsed');
		                $(this).find('i').removeClass('fa-sort-down').addClass('fa-sort-up');
		            }
		            else {
		                // collapse the panel
		                $(this).parents('.panel').find('.panel-body').slideUp();
		                $(this).addClass('panel-collapsed');
		                $(this).find('i').removeClass('fa-sort-up').addClass('fa-sort-down');
		            }
		        });
			 
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
					$(USED_TABLES_CONTAINER).css("height",$(this).height());
				}
			});
			$(queryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "width": "75%", "height":"150px"});
			
			this.processArgs(args);
			
			$(TOOLTIP_DIALOG).on('show.bs.modal', function () {
			       $(this).find('.modal-body').css({
			              width:'auto', //probably not needed
			              height:'auto', //probably not needed 
			              'max-height':'100%'
			       });
	        	});
			$(REFRESH_MENU).on('click', this.fetchExplainPlan);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);

			this.fetchExplainPlan();

		},
		doResume: function(args){
			common.redirectFlag=false;
			$(REFRESH_MENU).on('click', this.fetchExplainPlan);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			/*wHandler.on(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.on(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);*/
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
			if(queryID == null || queryID != args){
				this.processArgs(args);
				this.fetchExplainPlan();
			}
		},
		doPause: function(){
			common.redirectFlag=true;
			$(REFRESH_MENU).off('click', this.fetchExplainPlan);
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			/*wHandler.off(wHandler.CANCEL_QUERY_SUCCESS, this.cancelQuerySuccess);
			wHandler.off(wHandler.CANCEL_QUERY_ERROR, this.cancelQueryError);*/
			serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
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
				st.canvas.resize($('#infovis').width(), ($(GRAPH_CONTAINER).height() + $(GRAPH_CONTAINER).scrollTop()));
			}
		},
		processArgs: function(args){
			$('#query-id').val(args);
			queryID = args;
			queryType = null;
			queryTextEditor.setValue('');
			$(USED_TABLES_CONTAINER).empty();
			
			var queryParams = sessionStorage.getItem(queryID);
			sessionStorage.removeItem(queryID);
			if(queryParams != null){
				queryParams = JSON.parse(queryParams);
				queryType = queryParams.type;
				if(queryParams.text)
					queryTextEditor.setValue(queryParams.text);
			}				
		},
		fetchExplainPlan: function(){
			var queryText = queryTextEditor.getValue(); //$("#query-text").val();

			$("#infovis").hide();
			$("#errorText").hide();
			$(TEXT_PLAN_CONTAINER).hide();
			var param = {sQuery : queryText, sControlStmts: "", sQueryID: queryID, sQueryType: queryType};

			_this.showLoading();
			serverHandler.explainQuery(param);
			$(ERROR_CONTAINER).hide();
		},

		drawExplain: function (jsonData){
			_this.hideLoading();
			$('#text-result-container').show();
			$('#text-result').text(jsonData.planText);
			$("#infovis").empty();

			st = common.generateExplainTree(jsonData, setRootNode, _this.showExplainTooltip, $(GRAPH_CONTAINER));
			//load json data
			st.loadJSON(jsonData);

			$(TEXT_PLAN_CONTAINER).show();

			//compute node positions and layout
			st.compute();
			$("#infovis").show();
			//emulate a click on the root node.
			st.onClick(st.root);
			_this.populateTablesList(jsonData.tableNames);
			_this.doResize();	
		},
		populateTablesList: function(tablesList){
			if(tablesList == null || tablesList.length == 0){
				$(USED_TABLES_DIV).hide();
			}else{
				$(USED_TABLES_CONTAINER).empty();
				$(USED_TABLES_DIV).show();
				$.each(tablesList, function(k, v){
					var tableParts = common.crackSQLAnsiName(v.replace(/\"/g, ''));
					if(tableParts.length > 1){
						var link =	'<tr><td><a href="#/database/objdetail?type=table' 
						 		+ '&name=' + tableParts[tableParts.length -1] 
					 			+ '&schema='+ tableParts[tableParts.length -2]	            				 
	      			 			+ '">' + v + '</a></td><tr>';
	      			 	$(USED_TABLES_CONTAINER).append(link);
					}
				});				
			}
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
			var msgObj={msg:'The cancel query request has been submitted',tag:"success",url:_this.currentURL};
			if(common.redirectFlag==false){
				_this.popupNotificationMessage(msgObj);
			}else{
				
				common.fire(common.NOFITY_MESSAGE,msgObj);
			}
			/*alert('The cancel query request has been submitted');*/
		},
		cancelQueryError:function(jqXHR){
			var msgObj={msg:jqXHR.responseText,tag:"success",url:_this.currentURL};
			if(common.redirectFlag==false){
				_this.popupNotificationMessage(msgObj);
			}else{
				
				common.fire(common.NOFITY_MESSAGE,msgObj);
			}
			/*alert(jqXHR.responseText);*/
		},        

	});


	return QuerPlanView;
});
