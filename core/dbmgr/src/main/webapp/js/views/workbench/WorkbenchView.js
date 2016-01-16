// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
    'views/BaseView',
    'text!templates/workbench.html',
    'jquery',
    'common',
    'handlers/ServerHandler',
    '../../../bower_components/codemirror/lib/codemirror',
    '../../../bower_components/codemirror/mode/sql/sql',
    'jit',
    'datatables',
    'datatablesBootStrap',
    'tablebuttons',
    'buttonsflash',
    'buttonsprint',
    'buttonshtml'
], function (BaseView, WorkbenchT, $, common, serverHandler, CodeMirror) {
    'use strict';

    var setRootNode = false;
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var st = null;
    var resizeTimer = null;			
    var oDataTable = null;
    var controlStatements = null;
    var previousScrollTop = 0;
    var controlStmts = "";
    
    var CONTROL_DIALOG = '#controlDialog',
    	CONTROL_APPLY_BUTTON = "#controlApplyButton",
    	TOOLTIP_DIALOG = '#tooltipDialog',
    	TOOLTIP_DAILOG_LABEL = '#toolTipDialogLabel',
    	TOOLTIP_CONTAINER = '#tooltipContainer';
    
    var SPINNER = '#loadingImg',
    	PRIMARY_RESULT_CONTAINER = '#primary-result-container',
    	TEXT_RESULT_CONTAINER = '#text-result-container',
    	TEXT_RESULT = '#text-result',
    	EXPLAIN_TREE = '#infovis',
    	QUERY_RESULT_CONTAINER = '#query-result-container',
    	ERROR_TEXT = '#query-error-text',
    	SCALAR_RESULT_CONTAINER = '#scalar-result-container',
    	EXPLAIN_BTN = '#explainQuery',
    	EXECUTE_BTN = '#executeQuery',
    	OPTIONS_BTN = '#setControlStmts',
    	CLEAR_BTN = '#clearAction',
    	CONTROL_STMTS_TEXT = '#query-control-stmts',
    	QUERY_TEXT = '#query-text';
    	
    var _that = null;
    var queryTextEditor = null,
    	controlStmtEditor = null,
    	resultsDataTable = null;
    
    $jit.ST.Plot.NodeTypes.implement({
    	'nodeline': {
    	  'render': function(node, canvas, animating) {
    			if(animating === 'expand' || animating === 'contract') {
    			  var pos = node.pos.getc(true), nconfig = this.node, data = node.data;
    			  var width  = nconfig.width, height = nconfig.height;
    			  var algnPos = this.getAlignedPos(pos, width, height);
    			  var ctx = canvas.getCtx(), ort = this.config.orientation;
    			  ctx.beginPath();
    			  if(ort == 'left' || ort == 'right') {
    				  ctx.moveTo(algnPos.x, algnPos.y + height / 2);
    				  ctx.lineTo(algnPos.x + width, algnPos.y + height / 2);
    			  } else {
    				  ctx.moveTo(algnPos.x + width / 2, algnPos.y);
    				  ctx.lineTo(algnPos.x + width / 2, algnPos.y + height);
    			  }
    			  ctx.stroke();
    		  } 
    	  }
    	}
    });    			

    var WorkbenchView = BaseView.extend({

        template: _.template(WorkbenchT),

        showLoading: function(){
        	$(SPINNER).show();
        },

        hideLoading: function () {
        	$(SPINNER).hide();
        },
        				
        drawExplain: function (jsonData) {
        	_that.hideLoading();
        	$(TEXT_RESULT_CONTAINER).show();
        	$(TEXT_RESULT).text(jsonData.planText);
        	$(EXPLAIN_TREE).empty();

        	//init Spacetree
        	//Create a new ST instance
        	st = common.generateExplainTree(jsonData, setRootNode, _that.showExplainTooltip, $(PRIMARY_RESULT_CONTAINER));
        		
        	//load json data
        	st.loadJSON(jsonData);
        	//compute node positions and layout
        	st.compute();
        	$(EXPLAIN_TREE).show();
        	//emulate a click on the root node.
        	st.onClick(st.root);
        	_that.doResize();
        	//end
        },
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
        doInit: function () {
        	_that = this;
        	$(TEXT_RESULT_CONTAINER).hide();
        	$(SCALAR_RESULT_CONTAINER).hide();
        	this.hideLoading();
        	$(EXPLAIN_BTN).on('click', this.explainQuery);
        	$(EXECUTE_BTN).on('click', this.executeQuery);
        	$(CLEAR_BTN).on('click', this.clearAll);
        	
        	$(CONTROL_APPLY_BUTTON).on('click', this.controlApplyClicked);
        	$(OPTIONS_BTN).on('click', this.openFilterDialog);

        	$(EXPLAIN_TREE).hide();
        	$(ERROR_TEXT).hide();
        	$(TOOLTIP_DIALOG).on('show.bs.modal', function () {
		       $(this).find('.modal-body').css({
		              width:'auto', //probably not needed
		              height:'auto', //probably not needed 
		              'max-height':'100%'
		       });
        	});
        	
        	if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
        		common.defineEsgynSQLMime(CodeMirror);
        	}
        	
        	queryTextEditor = CodeMirror.fromTextArea(document.getElementById("query-text"), {
        	    mode: 'text/x-esgyndb',
        	    indentWithTabs: true,
        	    smartIndent: true,
        	    lineNumbers: true,
        	    lineWrapping: true,
        	    matchBrackets : true,
        	    autofocus: true,
        	    extraKeys: {"Ctrl-Space": "autocomplete"}
        	});
        	$(queryTextEditor.getWrapperElement()).resizable({
        		  resize: function() {
        			    queryTextEditor.setSize($(this).width(), $(this).height());
        			  }
        			});
        	$(queryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"150px"});
        	
        	controlStmtEditor = CodeMirror.fromTextArea(document.getElementById("query-control-stmts"), {
        	    mode: 'text/x-esgyndb',
        	    indentWithTabs: true,
        	    smartIndent: true,
        	    lineNumbers: true,
        	    matchBrackets : true,
        	    autofocus: false,
        	    lineWrapping: true,
        	    extraKeys: {"Ctrl-Space": "autocomplete"}
        	});
        	$(controlStmtEditor.getWrapperElement()).resizable({
      		  resize: function() {
      			    controlStmtEditor.setSize($(this).width(), $(this).height());
      			  }
      			});
        	$(controlStmtEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"300px"});
        	
        	$(CONTROL_DIALOG).on('hide.bs.modal', function(e){
        		if(controlStmts && controlStmts.length > 0){
	        		if(controlStmtEditor)
	    	    		 controlStmtEditor.setValue(controlStmts);
	    	    	else
	    	    		$(CONTROL_STMTS_TEXT).val(controlStmts);
        		}else{
	        		if(controlStmtEditor)
	    	    		 controlStmtEditor.setValue("");
	    	    	else
	    	    		$(CONTROL_STMTS_TEXT).val("");
       			
        		}
    		});
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
        },
        doResume: function(){
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
        },
        doPause:  function(){
        	this.hideLoading();
			serverHandler.off(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			serverHandler.off(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
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
        		st.canvas.resize($(EXPLAIN_TREE).width(), ($(PRIMARY_RESULT_CONTAINER).height() + $(PRIMARY_RESULT_CONTAINER).scrollTop()));;
        	}
        },

        openFilterDialog: function () {
        	$(CONTROL_DIALOG).modal('show');
        },
        controlApplyClicked: function(){
        	if(controlStmtEditor)
	    		controlStmts = controlStmtEditor.getValue();
	    	else
	    		controlStmts = $(CONTROL_STMTS_TEXT).val();
        	
        	if(controlStmts == null) {
				controlStmts = "";
			} else {
				controlStmts = controlStmts.replace(/(\r\n|\n|\r)/gm,"");
			}

        	if(controlStmts && controlStmts.length > 0){
        		$(OPTIONS_BTN).text(" Options ON");
        	}else{
        		$(OPTIONS_BTN).text(" Options");
        	}
			$(CONTROL_DIALOG).modal('hide')
        },

        parseControlStmts: function(){
        	if(controlStmtEditor)
	    		controlStmts = controlStmtEditor.getValue();
	    	else
	    		controlStmts = $(CONTROL_STMTS_TEXT).val();
        	if(controlStmts == null) {
				controlStmts = "";
			} else {
				controlStmts = controlStmts.replace(/(\r\n|\n|\r)/gm,"");
			}
        },
        clearAll: function(){
        	$(EXPLAIN_TREE).hide();
        	$(ERROR_TEXT).hide();
        	$(QUERY_RESULT_CONTAINER).hide();
        	$(TEXT_RESULT_CONTAINER).hide();
        	$(SCALAR_RESULT_CONTAINER).hide();
        	
        	if(queryTextEditor)
           	 	queryTextEditor.setValue("");
        	else
        		$(QUERY_TEXT).val("");
        	
        	if(controlStmtEditor)
	    		controlStmtEditor.setValue("");
	    	else
	    		$(CONTROL_STMTS_TEXT).val();
        	
        	if(resultsDataTable  != null){
        		try{
            		resultsDataTable.clear().draw();
        		}catch(err){
        			
        		}
        	}
        },
        explainQuery: function () {
        	var queryText = $(QUERY_TEXT).val();
        	if(queryTextEditor)
        	 queryText = queryTextEditor.getValue();
        	
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	
        	_that.parseControlStmts();
        	
        	$(EXPLAIN_TREE).hide();
        	$(ERROR_TEXT).hide();
        	$(QUERY_RESULT_CONTAINER).hide();
        	$(TEXT_RESULT_CONTAINER).hide();
        	$(SCALAR_RESULT_CONTAINER).hide();        	
        	var param = {sQuery : queryText, sControlStmts: controlStmts};

        	_that.showLoading();
        	serverHandler.explainQuery(param);
        	
        	/*$.ajax({
        	    url:'resources/queries/explain',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
				statusCode : {
					401 : _that.sessionTimeout,
					403 : _that.sessionTimeout
				},
				success:_that.drawExplain,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});*/
        	
            
        },

        executeQuery: function () {
        	var queryText = $(QUERY_TEXT).val();
        	if(queryTextEditor)
        	 queryText = queryTextEditor.getValue();
        	
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	_that.parseControlStmts();
        	
        	_that.showLoading();
        	$(EXPLAIN_TREE).hide();
        	$(ERROR_TEXT).hide();
        	$(TEXT_RESULT_CONTAINER).hide();
        	$(SCALAR_RESULT_CONTAINER).hide();
        	$(QUERY_RESULT_CONTAINER).hide();
        	var param = {sQuery : queryText, sControlStmts: controlStmts};
        	serverHandler.executeQuery(param);
        	
        	/*$.ajax({
        	    url:'resources/queries/execute',
        	    type:'POST',
        	    data: JSON.stringify(param),
        	    dataType:"json",
        	    contentType: "application/json;",
				statusCode : {
					401 : _that.sessionTimeout,
					403 : _that.sessionTimeout
				},
				success:_that.displayResults,
        	    error:function(jqXHR, res, error){
        	    	_that.hideLoading();
        	    	_that.showErrorMessage(jqXHR);
        	    }
        	});*/
        },
        
    	sessionTimeout: function() {
    		window.location.hash = '/stimeout';
    	},

    	displayResults: function (result){
        	_that.hideLoading();
        	var keys = result.columnNames;
        	if(result.isScalarResult != null && result.isScalarResult == true){
        		$(SCALAR_RESULT_CONTAINER).show();
        		$(SCALAR_RESULT_CONTAINER).text(result.resultArray[0][0]);
        	}
        	else{
            	$(QUERY_RESULT_CONTAINER).show();        	

            	if(keys != null && keys.length > 0) {
            		var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="wrkbnch-query-results"></table>';
            		$(QUERY_RESULT_CONTAINER).html( sb );
            		
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
            		
            		 $('#wrkbnch-query-results').dataTable({
            			 "oLanguage": {
            				 "sEmptyTable": "0 rows(s)"
            			},
            			dom:'lBftrip',
            			"bProcessing": true,
            			"iDisplayLength" : 25, 
				"sPaginationType": "full_numbers",
        		        "scrollCollapse": true,
            			"aaData": aaData, 
            			"aoColumns" : aoColumns,
            			paging: bPaging,
            			buttons: [
    					          'copy','csv','excel','pdf','print'
    				          ]
            		 });
            	}        		
        	}
        },
        
        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	$(EXPLAIN_TREE).hide();
        	$(QUERY_RESULT_CONTAINER).hide();
        	$(TEXT_RESULT_CONTAINER).hide();

        	$(ERROR_TEXT).show();
        	if (jqXHR.responseText) {
        		$(ERROR_TEXT).text(jqXHR.responseText);
        	}else{
        		if(jqXHR.status != null && jqXHR.status == 0) {
        			$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
        		}
        	}
        }        
    });

    return WorkbenchView;
});
