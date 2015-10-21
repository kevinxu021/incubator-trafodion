// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
    'views/BaseView',
    'text!templates/workbench.html',
    'jquery',
    'common',
    '../../../bower_components/codemirror/lib/codemirror',
    '../../../bower_components/codemirror/mode/sql/sql',
    'jit',
    'datatables',
    'datatablesBootStrap',
    'tabletools'
], function (BaseView, WorkbenchT, $, common, CodeMirror) {
    'use strict';

    var setRootNode = false;
    var LOADING_SELECTOR = ".dbmgr-spinner";			
    var st = null;
    var resizeTimer = null;			
    var GRIDCONTAINER = "#dbmgr-1";
    var oDataTable = null;
    var controlStatements = null;
    var previousScrollTop = 0;
    var controlStmts = "";
    var CONTROL_DIALOG = '#controlDialog',
    	CONTROL_APPLY_BUTTON = "#controlApplyButton",
    	TOOLTIP_DIALOG = '#tooltipDialog';
    var _that = null;
    var queryTextEditor = null,
    	controlStmtEditor = null;
    
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
        	$('#loadingImg').show();
        },

        hideLoading: function () {
        	$('#loadingImg').hide();
        },
        				
        drawExplain: function (jsonData) {
        	_that.hideLoading();
        	$('#text-result-container').show();
        	$('#text-result').text(jsonData.planText);
        	$("#infovis").empty();

        	//init Spacetree
        	//Create a new ST instance
        	st = common.generateExplainTree(jsonData, setRootNode, _that.showExplainTooltip);
        		
        	//load json data
        	st.loadJSON(jsonData);
        	//compute node positions and layout
        	st.compute();
        	$("#infovis").show();
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
        	$('#toolTipDialogLabel').text(nodeName);
        	$('#tooltipContainer').text(data);
        },
        toProperCase: function (s) {
        	return s.toLowerCase().replace(/^(.)|\s(.)/g, function($1) {
        		return $1.toUpperCase();
        	});
        },
        doInit: function () {
        	_that = this;
        	$('#text-result-container').hide();
        	$('#scalar-result-container').hide();
        	this.hideLoading();
        	$(CONTROL_APPLY_BUTTON).on('click', this.controlApplyClicked);
        	//initFilterDialog();
        	$("#explainQuery").on('click', this.explainQuery);
        	$("#executeQuery").on('click', this.executeQuery);
        	$("#setControlStmts").on('click', this.openFilterDialog);
        	$("#infovis").show();
        	$("#errorText").hide();
        	$('#tooltipDialog').on('show.bs.modal', function () {
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
        	    extraKeys: {"Ctrl-Space": "autocomplete"},
        	    hintOptions: {tables: {
        	      users: {name: null, score: null, birthDate: null},
        	      countries: {name: null, population: null, size: null}
        	    }}
        	});
        	
        	controlStmtEditor = CodeMirror.fromTextArea(document.getElementById("controlStmts"), {
        	    mode: 'text/x-esgyndb',
        	    indentWithTabs: true,
        	    smartIndent: true,
        	    lineNumbers: true,
        	    matchBrackets : true,
        	    autofocus: true,
        	    lineWrapping: true,
        	    extraKeys: {"Ctrl-Space": "autocomplete"},
        	    hintOptions: {tables: {
        	      users: {name: null, score: null, birthDate: null},
        	      countries: {name: null, population: null, size: null}
        	    }}
        	});
        },
        doResume: function(){
        	
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

        openFilterDialog: function () {
        	$(CONTROL_DIALOG).modal('show');
        },
        controlApplyClicked: function(){
        	//controlStmts = $("#controlStmts").val();
        	if(controlStmtEditor)
        		controlStmts = controlStmtEditor.getValue();
        	else
        		controlStmts = $("#controlStmts").val();
        	
			if(controlStmts == null) {
				controlStmts = "";
			} else {
				controlStmts = controlStmts.replace(/(\r\n|\n|\r)/gm,"");
			}
			$(CONTROL_DIALOG).modal('hide')
        },

        explainQuery: function () {
        	var queryText = $("#query-text").val();
        	if(queryTextEditor)
        	 queryText = queryTextEditor.getValue();
        	
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	$("#infovis").hide();
        	$("#errorText").hide();
        	$("#query-result-container").hide();
        	$('#text-result-container').hide();
        	$('#scalar-result-container').hide();        	
        	var param = {sQuery : queryText, sControlStmts: controlStmts};

        	_that.showLoading();
        	
        	$.ajax({
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
        	});
            
        },

        executeQuery: function () {
        	var queryText = $("#query-text").val();
        	if(queryTextEditor)
        	 queryText = queryTextEditor.getValue();
        	
        	if(queryText == null || queryText.length == 0){
        		alert('Query text cannot be empty.');
        		return;
        	}
        	_that.showLoading();
        	$("#infovis").hide();
        	$("#errorText").hide();
        	$('#text-result-container').hide();
        	$('#scalar-result-container').hide();
        	$("#query-result-container").hide();
        	var param = {sQuery : queryText, sControlStmts: controlStmts};
        	
        	$.ajax({
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
        	});
        },
        
    	sessionTimeout: function() {
    		window.location.hash = '/stimeout';
    	},

    	displayResults: function (result){
        	_that.hideLoading();
        	var keys = result.columnNames;
        	if(result.isScalarResult != null && result.isScalarResult == true){
        		$("#scalar-result-container").show();
        		$("#scalar-result-container").text(result.resultArray[0][0]);
        	}
        	else{
            	$("#query-result-container").show();        	

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
            		
            		 $('#query-results').dataTable({
            			 "oLanguage": {
            				 "sEmptyTable": "0 rows(s)"
            			},
            			 dom: 'T<"clear">lfrtip',
            			"bProcessing": true,
            			"bPaginate" : true, 
            			"iDisplayLength" : 25, 
            			"sPaginationType": "simple_numbers",
        		        "scrollCollapse": true,
            			"aaData": aaData, 
            			"aoColumns" : aoColumns,
            			paging: true,
        				"tableTools": {
    						"sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
    					}
            		 });
            	}        		
        	}
        },
        
        displayResultsOld: function (result){
        	_that.hideLoading();
        	var keys;
            $.each(result, function(i, data){
              keys = Object.keys(data);
        	});
            
        	if(keys != null && keys.length > 0) {
        		var sb = '<table class="table table-striped table-bordered table-hover" id="query-results"></table>';
        		$('#query-result-container').html( sb );
        		
        		var aoColumns = [];
        		var aaData = [];
        		
        		$.each(result, function(i, data){
        		var rowData = [];
        		  $.each(keys, function(k, v) {
        			rowData.push(data[v]);
        		  });
        		  aaData.push(rowData);
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
        			"iDisplayLength" : 25, 
        			"sPaginationType": "simple_numbers",
    		        "scrollCollapse": true,
        			"aaData": aaData, 
        			"aoColumns" : aoColumns,
        			paging: true,
					"tableTools": {
						"sSwfPath": "../bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					}
        		 });
        	 }
        },

        showErrorMessage: function (jqXHR) {
        	_that.hideLoading();
        	$("#infovis").hide();
        	$("#query-result-container").hide();
        	$('#text-result-container').hide();

        	$("#errorText").show();
        	if (jqXHR.responseText) {
        		$("#errorText").text(jqXHR.responseText);
        	}
        }        
    });

    return WorkbenchView;
});
