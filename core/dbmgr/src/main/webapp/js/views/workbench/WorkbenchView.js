//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/workbench.html',
        'jquery',
        'common',
        'handlers/ServerHandler',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'jit',
        'datatables.net',
        'datatables.net-bs',
        'datatables.net-buttons',
        'buttonsflash',
        'buttonsprint',
        'buttonshtml',
        'pdfmake'
        ], function (BaseView, WorkbenchT, $, common, serverHandler, CodeMirror) {
	'use strict';

	var setRootNode = false;
	var LOADING_SELECTOR = ".dbmgr-spinner";			
	var st = null;
	var oDataTable = null;
	var controlStatements = null;
	var previousScrollTop = 0;
	var controlStmts = "";
	var timeStamps={runTime:null,planTime:null};

	var CONTROL_DIALOG = '#controlDialog',
	QCANCEL_MENU = '#cancelAction',
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
	SCALAR_RESULT = '#scalar-result',
	EXPLAIN_BTN = '#explainQuery',
	EXECUTE_BTN = '#executeQuery',
	OPTIONS_BTN = '#setControlStmts',
	CLEAR_BTN = '#clearAction',
	CONTROL_STMTS_TEXT = '#query-control-stmts',
	QUERY_TEXT = '#query-text',
	EXPLAIN_JSON_DATA=null,
	IMPORT_QUERY='#importQuery',
	FILE_SELECT='#jsonFile',
	EXPORT_QUERY='#exportQuery';

	var _this = null;
	var queryTextEditor = null,
	controlStmtEditor = null,
	//scalarResultEditor = null,
	resultsDataTable = null,
	resultsAfterPause = false,
	lastExecuteResult = null,
	lastExplainResult = null,
	lastRawError = null;
	
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
		
		explainJsonData:null,
		
		template: _.template(WorkbenchT),

		showLoading: function(){
			$(SPINNER).show();
		},

		hideLoading: function () {
			$(SPINNER).hide();
		},

		drawExplain: function (jsonData,tag) {
			EXPLAIN_JSON_DATA=jsonData;
			if(jsonData.requestor !=null && jsonData.requestor != _this) //error message is probably for different page
				{
				if(tag!="import"){
					return;
					}
				}
			
			if(this.redirectFlag){
				resultsAfterPause = true;
				lastExplainResult = jsonData;
				var msgObj={msg:'The workbench query explain completed successfully.',tag:"success",url:_this.currentURL,shortMsg:"Workbench explain succeeded.",lastMessageOnly:true};
				common.fire(common.NOFITY_MESSAGE,msgObj);
				return;
			}
			_this.hideLoading();
			$(EXECUTE_BTN).attr("disabled",false);
			$(TEXT_RESULT_CONTAINER).show();
			$(TEXT_RESULT).text(jsonData.planText);
			$(EXPLAIN_TREE).empty();

			//init Spacetree
			//Create a new ST instance
			st = common.generateExplainTree(jsonData, setRootNode, _this.showExplainTooltip, $(PRIMARY_RESULT_CONTAINER));

			//load json data
			st.loadJSON(jsonData);
			//compute node positions and layout
			st.compute();
			$(EXPLAIN_TREE).show();
			//emulate a click on the root node.
			st.onClick(st.root);
			_this.handleWindowResize();
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
			_this = this;
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			$(TEXT_RESULT_CONTAINER).hide();
			$(SCALAR_RESULT_CONTAINER).hide();
			this.hideLoading();
			$(EXPLAIN_BTN).on('click', this.explainQuery);
			$(EXECUTE_BTN).on('click', this.executeQuery);
			$(CLEAR_BTN).on('click', this.clearAll);
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(EXPORT_QUERY).on('click',this.exportQuery);
			$(FILE_SELECT).on('change',this.importQuery);

			$(CONTROL_APPLY_BUTTON).on('click', this.controlApplyClicked);
			$(OPTIONS_BTN).on('click', this.openFilterDialog);

			resultsDataTable = null;
			resultsAfterPause = false;
			lastExecuteResult = null;
			lastExplainResult = null;
			lastRawError = null;
			$(EXPLAIN_TREE).hide();
			$(ERROR_TEXT).hide();
			$(TOOLTIP_DIALOG).on('show.bs.modal', function () {
				$(this).find('.modal-body').css({
					width:'auto', 
					height:'auto',
					'max-height':'100%'
				});
			});
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
				indentWithTabs: true,
				smartIndent: true,
				lineNumbers: true,
				lineWrapping: true,
				matchBrackets : true,
				autofocus: true,
				styleSelectedText: true,
				extraKeys: {"Ctrl-Space": "autocomplete"}
			});
			$(queryTextEditor.getWrapperElement()).resizable({
				resize: function() {
					queryTextEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(queryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"120px", "font-size":"12px"});
			
			controlStmtEditor = CodeMirror.fromTextArea(document.getElementById("query-control-stmts"), {
				mode: 'text/x-esgyndb',
				indentWithTabs: true,
				smartIndent: true,
				lineNumbers: false,
				lineWrapping: true,
				matchBrackets : true,
				autofocus: true,
				extraKeys: {"Ctrl-Space": "autocomplete"}
			});

			
			$(controlStmtEditor.getWrapperElement()).resizable({
				resize: function() {
					controlStmtEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(controlStmtEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"300px", "font-size":"12px"});

			
			/*scalarResultEditor = CodeMirror.fromTextArea(document.getElementById("scalar-result"), {
				mode: 'text/x-esgyndb',
				indentWithTabs: true,
				smartIndent: true,
				lineNumbers: false,
				lineWrapping: true,
				matchBrackets : true,
				autofocus: true,
				extraKeys: {"Ctrl-Space": "autocomplete"}
			});
			$(scalarResultEditor.getWrapperElement()).resizable({
				resize: function() {
					scalarResultEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(scalarResultEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"300px"});*/
			
			$(CONTROL_DIALOG).on('hide.bs.modal', function(e){
				if(controlStmts && controlStmts.length > 0){
					if(controlStmtEditor)
						controlStmtEditor.setValue(controlStmts);
					else
						$(CONTROL_STMTS_TEXT).val(controlStmts);
				}else{
					if(controlStmtEditor){
						controlStmtEditor.setValue("");
						setTimeout(function() {
							controlStmtEditor.refresh();
		        		},1);
					}
					else
						$(CONTROL_STMTS_TEXT).val("");

				}
			});
			
			_this.clearAll();
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.WRKBNCH_CANCEL_SUCCESS, this.handleMessage);
			serverHandler.on(serverHandler.WRKBNCH_CANCEL_ERROR, this.handleMessage);
		},
		doResume: function(){
			$(QCANCEL_MENU).on('click', this.cancelQuery);
			$(EXPORT_QUERY).on('click',this.exportQuery);
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			if(resultsAfterPause == true){
				if(lastExecuteResult != null){
					_this.displayResults(lastExecuteResult);
				}else if (lastExplainResult != null){
					_this.drawExplain(lastExplainResult);
				}else if (lastRawError != null){
					_this.showErrorMessage(lastRawError);
				}
			}
			//serverHandler.on(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			//serverHandler.on(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			//serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			//serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
		},
		doPause:  function(){
			$(QCANCEL_MENU).off('click', this.cancelQuery);
			$(EXPORT_QUERY).off('click',this.exportQuery);
			this.redirectFlag=true;
			//this.hideLoading();
			//serverHandler.off(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			//serverHandler.off(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			//serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			//serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
		},
		handleMessage:function(data){
			if(data==true){
				var msgObj={msg:'The workbench query was canceled successfully.',tag:"success",url:_this.currentURL,shortMsg:"Workbench query canceled successfully."};
				if(_this.redirectFlag){
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}else{
					_this.popupNotificationMessage(null,msgObj);
				}
			}else{
				var msgObj={msg:'The workbench query was completed, could not be canceled.',tag:"warning",url:_this.currentURL,shortMsg:"Workbench query completed."};
				if(_this.redirectFlag){
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}else{
					_this.popupNotificationMessage(null,msgObj);
				}
			}
		},
		handleWindowResize: function () {
			if(st != null) {
				st.canvas.resize($(EXPLAIN_TREE).width(), ($(PRIMARY_RESULT_CONTAINER).height() + $(PRIMARY_RESULT_CONTAINER).scrollTop()));
			}
		},
		exportQuery:function(){
			var queryText = $(QUERY_TEXT).val();
			var planText=$(TEXT_RESULT).text();
			if(queryTextEditor){
				queryText = queryTextEditor.getSelection();
				if(queryText.length == 0){
					queryText = queryTextEditor.getValue();
				}
			}
			var controlStatement=$(CONTROL_STMTS_TEXT).val();
			if(controlStmtEditor)
				controlStatement = controlStmtEditor.getValue();
			else
				controlStatement = $(CONTROL_STMTS_TEXT).val();
			if(controlStatement == null) {
				controlStatement = "";
			} else {
				controlStatement = controlStatement.replace(/(\r\n|\n|\r)/gm,"");
			}
			/*var isSame=planText.indexOf(queryText);
			if(isSame==-1){
				EXPLAIN_JSON_DATA={};
			}*/
			var json={queryText:queryText,EXPLAIN_JSON_DATA:EXPLAIN_JSON_DATA,controlStatement:controlStatement};
			_this.SaveDatFileBro(json);
		},
		SaveDatFileBro:function(json) {
			var data = "text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(json));
			$('<a id="downloadJson" href="data:' + data + '" download="export.json" style="display:none">download JSON</a>').appendTo('body');
			$("#downloadJson")[0].click();
			$("#downloadJson").remove();
		},
		importQuery:function(event)
		{
			var files = event.target.files;
			var __this = _this;
			var json = files[0];
				if (window.File && window.FileReader && window.FileList && window.Blob) {
				  /*console.log("Great success! All the File APIs are supported.");*/
				} else {
				  console.log('The File APIs are not fully supported in this browser.');
				}
			var reader = new FileReader();
			var resultJson=null;
			reader.onload=function(progressEvent){
				resultJson=JSON.parse(this.result);
				queryTextEditor.setValue(resultJson.queryText);
				if(jQuery.isEmptyObject(resultJson.EXPLAIN_JSON_DATA)!=true){
					__this.drawExplain(resultJson.EXPLAIN_JSON_DATA,"import");
					$(EXPLAIN_TREE).show();
					$(TEXT_RESULT_CONTAINER).show();
				}else{
					$(EXPLAIN_TREE).hide();
					$(TEXT_RESULT_CONTAINER).hide();
				}
				controlStmtEditor.setValue(resultJson.controlStatement);
				if(resultJson.controlStatement!=""){
					_this.controlApplyClicked();
				}
			};
			reader.readAsText(json);
		},
		cancelQuery: function(){
			var param=null;
			if($(EXPLAIN_BTN).attr("disabled")=="disabled"){
				param={timeStamp:timeStamps.runTime};
			}else if($(EXECUTE_BTN).attr("disabled")=="disabled"){
				param={timeStamp:timeStamps.planTime};
			}else{
				param={timeStamp:timeStamps.runTime};
			}
			serverHandler.cancelQuery(param);
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
			resultsAfterPause = false;
			lastExecuteResult = null;
			lastExplainResult = null;
			lastRawError = null;	
			
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
			
			$(SCALAR_RESULT).text("");
			$(OPTIONS_BTN).text(" Options");
			if(resultsDataTable  != null){
				try{
					resultsDataTable.clear().draw();
				}catch(err){

				}
			}
		},
		explainQuery: function () {
			resultsAfterPause = false;
			lastExecuteResult = null;
			lastExplainResult = null;
			lastRawError = null;
			
			var queryText = $(QUERY_TEXT).val();
			if(queryTextEditor){
				queryText = queryTextEditor.getSelection();
				if(queryText.length == 0){
					queryText = queryTextEditor.getValue();
				}
			}

			if(queryText == null || queryText.length == 0){
				alert('Query text cannot be empty.');
				return;
			}

			_this.parseControlStmts();

			$(EXPLAIN_TREE).hide();
			$(ERROR_TEXT).hide();
			$(QUERY_RESULT_CONTAINER).hide();
			$(TEXT_RESULT_CONTAINER).hide();
			$(SCALAR_RESULT_CONTAINER).hide();        	
			$(EXECUTE_BTN).attr("disabled",true);
			timeStamps.planTime=new Date().getTime();
			var param = {sQuery : queryText, sControlStmts: controlStmts,timeStamp:timeStamps.planTime};

			_this.showLoading();
			serverHandler.explainQuery(param, _this);
		},

		executeQuery: function () {
			lastExecuteResult = null;
			lastExplainResult = null;
			resultsAfterPause = false;
			lastRawError = null;
			timeStamps.runTime=new Date().getTime();
			
			var queryText = $(QUERY_TEXT).val();
			if(queryTextEditor){
				queryText = queryTextEditor.getSelection();
				if(queryText.length == 0){
					queryText = queryTextEditor.getValue();
				}
			}

			if(queryText == null || queryText.length == 0){
				alert('Query text cannot be empty.');
				return;
			}
			_this.parseControlStmts();

			_this.showLoading();
			$(EXPLAIN_TREE).hide();
			$(EXPLAIN_TREE).empty();
			$("#text-result").empty();
			$(ERROR_TEXT).hide();
			$(TEXT_RESULT_CONTAINER).hide();
			$(SCALAR_RESULT_CONTAINER).hide();
			$(QUERY_RESULT_CONTAINER).hide();
			$(EXPLAIN_BTN).attr("disabled",true);
			var param = {sQuery : queryText, sControlStmts: controlStmts,timeStamp:timeStamps.runTime};
			serverHandler.executeQuery(param);
		},

		sessionTimeout: function() {
			window.location.hash = '/stimeout';
		},

		displayResults: function (result){
			$(EXPLAIN_BTN).attr("disabled",false);
			if(_this.redirectFlag){
				resultsAfterPause = true;
				lastExecuteResult = result;
				var msgObj={msg:'The workbench query execution completed successfully.',tag:"success",url:_this.currentURL,shortMsg:"Workbench execute succeeded.",lastMessageOnly:true};
				common.fire(common.NOFITY_MESSAGE,msgObj);
				return;
			}
			_this.hideLoading();
			
			var keys = result.columnNames;
			if(result.isScalarResult != null && result.isScalarResult == true){
				$(SCALAR_RESULT_CONTAINER).show();
				$(SCALAR_RESULT).text(result.resultArray[0][0]);
				//scalarResultEditor.setValue(result.resultArray[0][0]);
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

					$('#wrkbnch-query-results').DataTable({
						"oLanguage": {
							"sEmptyTable": "0 rows(s)"
						},
						//dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
						dom: "<'row'<'col-md-8'lB><'col-md-4'f>>" +"<'row'<'col-md-12'<'datatable-scroll'tr>>><'row'<'col-md-12'ip>>",
						processing: true,
						"iDisplayLength" : 25, 
						"sPaginationType": "full_numbers",
						"scrollCollapse": true,
						"aaData": aaData, 
						"aoColumns" : aoColumns,
						paging: bPaging,
						buttons: [
		                           { extend : 'copy', exportOptions: { columns: ':visible' } },
		                           { extend : 'csv', exportOptions: { columns: ':visible' } },
		                           //{ extend : 'excel', exportOptions: { columns: ':visible' } },
		                           { extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, 
		                        	   title: 'Query Workbench' } ,
		                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Query Workbench' },
	                        	   {text: 'JSON',
	                        		   action: function ( e, dt, button, config ) {
	                        			   var data = dt.buttons.exportData();
	                        			   $.fn.dataTable.fileSave(
	                        					   new Blob( [ JSON.stringify( data ) ] ),
	                        					   'Query Workbench.json'
	                        			   );
	                        		   }}
					          ]

					});
				}        		
			}
		},

		showErrorMessage: function (jqXHR) {
			if(jqXHR.requestor !=null && jqXHR.requestor != _this) //error message is probably for different page
				return;
			
			if(_this.redirectFlag){
				resultsAfterPause = true;
				lastRawError = jqXHR;
				var msgObj={msg:'The workbench operation failed.',tag:"danger",url:_this.currentURL,shortMsg:"Workbench operation failed.",lastMessageOnly:true};
				common.fire(common.NOFITY_MESSAGE,msgObj);
				return;
			}
			_this.hideLoading();
			$(EXPLAIN_BTN).attr("disabled",false);
			$(EXECUTE_BTN).attr("disabled",false);
			$(EXPLAIN_TREE).hide();
			$(QUERY_RESULT_CONTAINER).hide();
			$(TEXT_RESULT_CONTAINER).hide();

			$(ERROR_TEXT).show();
			if (jqXHR.responseText) {
				$(ERROR_TEXT).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(ERROR_TEXT).text("Error : Unable to communicate with the server.");
				}
			}
		}        
	});

	return WorkbenchView;
});
