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
        'pdfmake',
        'jqueryscroll'
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
	var isCancelClicked=false;
	var executionQueryText=null;
	var explainQueryText=null;

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
	VISUAL_EXPLAIN_CONTAINER = '#visual-explain-container',
	EXPLAIN_BTN = '#explainQuery',
	EXECUTE_BTN = '#executeQuery',
	OPTIONS_BTN = '#setControlStmts',
	CLEAR_BTN = '#clearAction',
	CONTROL_STMTS_TEXT = '#query-control-stmts',
	QUERY_TEXT = '#query-text',
	EXPLAIN_JSON_DATA=null,
	IMPORT_QUERY='#importQuery',
	QUERY_FILE='#queryFile',
	IMPORT_WBJ='#importWbj',
	WBJ_FILE='#wbjFile',
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

		drawExplain: function (jsonData, tag) {
			EXPLAIN_JSON_DATA=jsonData;
			if(jsonData.requestor !=null && jsonData.requestor != _this) //error message is probably for different page
			{
				if(tag != "import"){
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
			$(VISUAL_EXPLAIN_CONTAINER).show();
			$(TEXT_RESULT_CONTAINER).show();
			$(TEXT_RESULT).text(jsonData.planText);
			$(EXPLAIN_TREE).empty();

			//init Spacetree
			//Create a new ST instance
			st = common.generateExplainTree(jsonData, setRootNode, _this.showExplainTooltip, $(VISUAL_EXPLAIN_CONTAINER));

			//load json data
			st.loadJSON(jsonData);
			//compute node positions and layout
			st.compute();
			$(EXPLAIN_TREE).show();
			//emulate a click on the root node.
			st.onClick(st.root);
			_this.handleWindowResize();
			
			if(tag == "import"){
				_this.displayPanel($(VISUAL_EXPLAIN_CONTAINER));
			}
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
			$(VISUAL_EXPLAIN_CONTAINER).hide();
			$(TEXT_RESULT_CONTAINER).hide();
			$(SCALAR_RESULT_CONTAINER).hide();
			this.hideLoading();
			$(EXPLAIN_BTN).on('click', this.explainQuery);
			$(EXECUTE_BTN).on('click', this.executeQuery);
			$(CLEAR_BTN).on('click', this.clearAll);
			$(EXPORT_QUERY).on('click',this.exportQuery);
			$(WBJ_FILE).on('click',this.importBtnClicked);
			$(QUERY_FILE).on('change',this.importQuery);
			$(WBJ_FILE).on('change',this.importQuery);

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
			$(CONTROL_DIALOG).on('shown.bs.modal', function(e){
				setTimeout(function() {
					controlStmtEditor.refresh();
				},1);
			});			
			_this.clearAll();
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			serverHandler.on(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
			$('[data-toggle="tooltip"]').tooltip({
				trigger : 'hover',
				container : "body",
				html : true
			}).css('overflow', 'hidden');
		},
		doResume: function(){
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
			$(EXPORT_QUERY).off('click',this.exportQuery);
			this.redirectFlag=true;
			//this.hideLoading();
			//serverHandler.off(serverHandler.WRKBNCH_EXECUTE_SUCCESS, this.displayResults);
			//serverHandler.off(serverHandler.WRKBNCH_EXECUTE_ERROR, this.showErrorMessage);
			//serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_SUCCESS, this.drawExplain);
			//serverHandler.off(serverHandler.WRKBNCH_EXPLAIN_ERROR, this.showErrorMessage);
		},
		handleWindowResize: function () {
			if(st != null) {
				st.canvas.resize($(EXPLAIN_TREE).width(), ($(VISUAL_EXPLAIN_CONTAINER).height() + $(VISUAL_EXPLAIN_CONTAINER).scrollTop()));
			}
		},
		exportQuery:function(){
			var queryText;
			if(executionQueryText!=null){
				queryText=executionQueryText;
			}else{
				if(queryTextEditor){
					queryText = queryTextEditor.getSelection();
					if(queryText.length == 0){
						queryText = queryTextEditor.getValue();
					}
				}else{
					queryText=$(QUERY_TEXT).val();
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
			if(!$.isEmptyObject(EXPLAIN_JSON_DATA)){
				if(executionQueryText != explainQueryText){
					queryText=explainQueryText;
				}
				if(queryText==""&&controlStatement==""){
					return;
				}
			}
			var json={queryText:queryText, EXPLAIN_JSON_DATA:EXPLAIN_JSON_DATA, controlStatement:controlStatement, executionResults:lastExecuteResult};
			_this.SaveDatFileBro(json);
		},
		SaveDatFileBro:function(json) {
			var blob = new Blob([JSON.stringify(json)], {type: "text/plain;charset=utf-8"});
			saveAs(blob, "WorkbenchExport.wbj");
		},
		importBtnClicked: function(){
			this.value = null;
		},
		importQuery:function(event)
		{
			var files = event.target.files;
			var __this = _this;
			var file = files[0];
			if (window.File && window.FileReader && window.FileList && window.Blob) {
				/*console.log("Great success! All the File APIs are supported.");*/
			} else {
				console.log('The File APIs are not fully supported in this browser.');
			}
			var ext = file.name.split('.').pop();
			if(ext == file.name){
				ext = '';
			}
			switch(ext){
			case '':
			case 'txt':
			case 'sql':
			case 'ddl':
			case 'wbj':
				_this.clearAll();
				break;
				default:{
					var errMessage = {msg: ext + ' file type is not supported. Supported file types are .txt, .ddl, .sql, .wbj',tag:"danger",url:null,shortMsg:""};
					_this.popupNotificationMessage(null,errMessage);
					return;
				}
			}
			var reader = new FileReader();
			var result=null;
			reader.onload=(function(file){
				return function(e){
					var extension=file.name.split('.').pop();
					if(extension == file.name){
						extension = '';
					}
					switch (extension) {
					case '':
					case 'sql':
					case 'txt':
					case 'ddl':{
						
						result=e.target.result;
						queryTextEditor.setValue(result);
						$(OPTIONS_BTN).text(" ");
						$(EXPLAIN_TREE).hide();
						$(VISUAL_EXPLAIN_CONTAINER).hide();
						$(TEXT_RESULT_CONTAINER).hide();
						break;
					};
					case 'wbj':{
						_this.clearAll();
						result=JSON.parse(e.target.result);
						queryTextEditor.setValue(result.queryText);
						executionQueryText = result.queryText;
						explainQueryText = result.queryText;

						if(jQuery.isEmptyObject(result.EXPLAIN_JSON_DATA)!=true){
							__this.drawExplain(result.EXPLAIN_JSON_DATA, "import");
							$(EXPLAIN_TREE).show();
							$(TEXT_RESULT_CONTAINER).show();
						}else{
							$(VISUAL_EXPLAIN_CONTAINER).hide();
							$(EXPLAIN_TREE).hide();
							$(TEXT_RESULT_CONTAINER).hide();
						}
						controlStmtEditor.setValue(result.controlStatement);
						if(result.controlStatement!=""){
							_this.controlApplyClicked();
						}else if(result.controlStatement==""){
							$(OPTIONS_BTN).text(" ");
						}
						if(result.executionResults != ""){
							_this.displayResults(result.executionResults, "import");
						}
						break;
					}
					default: {
						alert("")
					}

					}};
			})(file);
			reader.readAsText(file);
		},
		cancelQuery: function(){
			isCancelClicked=true;
			var param=null;
			$(EXECUTE_BTN).trigger("mouseleave");
			$(EXECUTE_BTN).attr("disabled",true);
			param={timeStamp:timeStamps.runTime};
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
				$(OPTIONS_BTN).text(" ON");
			}else{
				$(OPTIONS_BTN).text(" ");
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
			executionQueryText=null;
			explainQueryText=null;

			$(EXPLAIN_TREE).hide();
			$(ERROR_TEXT).hide();
			$(VISUAL_EXPLAIN_CONTAINER).hide();
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
			$(OPTIONS_BTN).text(" ");
			if(resultsDataTable  != null){
				try{
					resultsDataTable.clear().draw();
				}catch(err){

				}
			}
		},
		explainQuery: function () {
			resultsAfterPause = false;
			lastExplainResult = null;
			lastRawError = null;

			var queryText = $(QUERY_TEXT).val();
			if(queryTextEditor){
				queryText = queryTextEditor.getSelection();
				if(queryText.length == 0){
					queryText = queryTextEditor.getValue();
				}
			}
			explainQueryText=queryText;
			if(queryText == null || queryText.length == 0){
				alert('Query text cannot be empty.');
				return;
			}

			if(executionQueryText != queryText){
				lastExecuteResult = "";
				$(QUERY_RESULT_CONTAINER).hide();
				$(SCALAR_RESULT_CONTAINER).hide();        	
			}

			_this.parseControlStmts();

			$(EXPLAIN_TREE).hide();
			$(ERROR_TEXT).hide();
			$(VISUAL_EXPLAIN_CONTAINER).hide();
			$(TEXT_RESULT_CONTAINER).hide();
			$(EXECUTE_BTN).attr("disabled",true);
			timeStamps.planTime=new Date().getTime();
			var param = {sQuery : queryText, sControlStmts: controlStmts,timeStamp:timeStamps.planTime};

			_this.showLoading();
			serverHandler.explainQuery(param, _this);
		},

		executeQuery: function () {
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
			}else{
				$(EXECUTE_BTN).attr("data-original-title","Cancel");
				$(EXECUTE_BTN).removeClass('btn-primary fa-play').addClass('btn-danger fa-stop');
				$(EXECUTE_BTN).unbind("click").on("click",_this.cancelQuery);
			}


			resultsAfterPause = false;
			lastRawError = null;
			isCancelClicked=false;
			timeStamps.runTime=new Date().getTime();

			executionQueryText=queryText;

			if(explainQueryText != queryText){
				lastExplainResult = "";
				EXPLAIN_JSON_DATA={};
				$(VISUAL_EXPLAIN_CONTAINER).hide();
				$(TEXT_RESULT_CONTAINER).hide();
				$(EXPLAIN_TREE).hide();
				$(EXPLAIN_TREE).empty();
				$("#text-result").empty();
			}

			_this.parseControlStmts();

			_this.showLoading();
			$(ERROR_TEXT).hide();
			$(SCALAR_RESULT_CONTAINER).hide();
			$(QUERY_RESULT_CONTAINER).hide();
			$(EXPLAIN_BTN).attr("disabled",true);
			var param = {sQuery : queryText, sControlStmts: controlStmts,timeStamp:timeStamps.runTime};
			serverHandler.executeQuery(param);
		},

		sessionTimeout: function() {
			window.location.hash = '/stimeout';
		},

		displayResults: function (result, tag){
			lastExecuteResult = result;

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

					resultsDataTable = $('#wrkbnch-query-results').DataTable({
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
						          { extend : 'csv', exportOptions: { columns: ':visible' }, filename: 'Query Workbench Results' },
						          //{ extend : 'excel', exportOptions: { columns: ':visible' } },
						          //{ extend : 'pdfHtml5', orientation: 'landscape', exportOptions: { columns: ':visible' }, title: 'Query Workbench Results' } ,
						          { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Query Workbench Results' }
						          ]

					});
				}        		
			}
			$(EXECUTE_BTN).trigger("mouseleave");
			$(EXECUTE_BTN).attr("data-original-title","Execute");
			$(EXECUTE_BTN).removeClass("btn-danger fa-stop").addClass("btn-primary fa-play");
			$(EXPLAIN_BTN).attr("disabled",false);
			$(EXECUTE_BTN).unbind("click").on("click",_this.executeQuery);
			if(isCancelClicked==true){
				var cancelMesObj={msg:'The workbench query was completed, could not be canceled.',tag:"warning",url:_this.currentURL,shortMsg:"Workbench query could not be canceled."};
				if(_this.redirectFlag){
					common.fire(common.NOFITY_MESSAGE,cancelMesObj);
				}else{
					_this.popupNotificationMessage(null,cancelMesObj);
				}
			}
			if($(EXECUTE_BTN).attr("disabled")=="disabled"){
				$(EXECUTE_BTN).attr("disabled",false);
			}

			if(tag == "import"){
				_this.displayPanel($(QUERY_RESULT_CONTAINER));
			}
		},
		displayPanel: function(tPanel){
			$('#content-wrapper').scrollTo(tPanel, 800);
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
			$(TEXT_RESULT_CONTAINER).hide();
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
			$(EXECUTE_BTN).trigger("mouseleave");
			$(EXECUTE_BTN).attr("data-original-title","Execute");
			$(EXECUTE_BTN).removeClass("btn-danger fa-stop").addClass("btn-primary fa-play");	
			$(EXECUTE_BTN).unbind("click").on("click",_this.executeQuery);
			if(isCancelClicked==true){
				var cancelMesObj=null;
				if (jqXHR.responseText.indexOf("ERROR[8007] The operation has been canceled")>-1) {
					cancelMesObj={msg:'Cancel request has been submitted.',tag:"success",url:_this.currentURL,shortMsg:"Cancel request has been submitted."};
				}else{
					cancelMesObj={msg:'there is no query to cancel, maybe errors occurs during execution.',tag:"warning",url:_this.currentURL,shortMsg:"Workbench Query could not be canceled."};
				}
				if(_this.redirectFlag){
					common.fire(common.NOFITY_MESSAGE,cancelMesObj);
				}else{
					_this.popupNotificationMessage(null,cancelMesObj);
				}
			}
			if($(EXECUTE_BTN).attr("disabled")=="disabled"){
				$(EXECUTE_BTN).attr("disabled",false);
			}
		}        
	});

	return WorkbenchView;
});
