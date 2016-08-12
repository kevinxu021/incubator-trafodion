//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/scriptexecute.html',
        'jquery',
        'common',
        'handlers/ServerHandler',
        'moment',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'filesaver'
        ], function (BaseView, ScriptExecutorT, $, common, serverHandler, moment, CodeMirror) {
	'use strict';

	var SPINNER = '#loadingImg';
	var SRC_FILE_NAME = "#source_file_name",
	SRC_FILE_SELECT = "#src_file",
	CLEAR_BTN = '#clearBtn',
	SAVE_BTN = '#saveBtn',
	EXEC_BTN = '#executeBtn',
	EXEC_RESULTS = '#exec-result';
	var resizeTimer = null;			

	var _this = null;
	var srcQueryTextEditor = null,
	isPaused = false,
	resultsAfterPause = false,
	lastExecuteResult = null,
	lastRawError = null;
	var mode = '';
	var execStartTime = null;
	
	var ScriptExecutorView = BaseView.extend({

		template: _.template(ScriptExecutorT),

		showLoading: function(){
			$(SPINNER).show();
		},

		hideLoading: function () {
			$(SPINNER).hide();
		},
		doInit: function (args) {
			_this = this;
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			this.hideLoading();
			resultsAfterPause = false;
			lastExecuteResult = null;
			lastRawError = null;
			
			mode = '';
			
			$(SRC_FILE_SELECT).on('change', this.onFileSelected);
			$(CLEAR_BTN).on('click',this.clearAll);
			$(SAVE_BTN).on('click', this.saveSQL);
			$(EXEC_BTN).on('click', this.executeBatchSQL);
						
			serverHandler.on(serverHandler.BATCH_SQL_SUCCESS, this.batchExecSuccess);
			serverHandler.on(serverHandler.BATCH_SQL_FAILURE, this.showErrorMessage);
			
			if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
				common.defineEsgynSQLMime(CodeMirror);
			}

			srcQueryTextEditor = CodeMirror.fromTextArea(document.getElementById("script-query-text"), {
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
			$(srcQueryTextEditor.getWrapperElement()).resizable({
				resize: function() {
					srcQueryTextEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(srcQueryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"200px", "font-size":"12px"});
			_this.clearAll();
			
			_this.processArgs();

			},
		doResume: function(args){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			isPaused = false;
			if(resultsAfterPause == true){
				if(lastExecuteResult != null){
					_this.batchExecSuccess(lastExecuteResult);
				}else if (lastRawError != null){
					_this.showErrorMessage(lastRawError);
				}
			}
			$(SRC_FILE_SELECT).on('change', this.onFileSelected);
			$(CLEAR_BTN).on('click',this.clearAll);
			$(SAVE_BTN).on('click', this.saveSQL);
			$(EXEC_BTN).on('click', this.executeBatchSQL);
			_this.processArgs();

		},
		doPause:  function(){
			isPaused = true;
			this.redirectFlag=true;
			$(SRC_FILE_SELECT).off('change', this.onFileSelected);
			$(CLEAR_BTN).off('click',this.clearAll);
			$(SAVE_BTN).off('click', this.saveSQL);
			$(EXEC_BTN).off('click', this.executeBatchSQL);
		},
		onRelayout: function () {
			this.onResize();
		},
		onResize: function () {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(this.doResize, 200);
		},
		doResize: function () {
		},
		clearAll: function(){
			if(srcQueryTextEditor)
				srcQueryTextEditor.setValue("");
			$(SRC_FILE_NAME).val("");
			$(EXEC_RESULTS).text("");
		},
		processArgs: function(){
			var batchQueryText = sessionStorage.getItem("executeBatch");
			if(batchQueryText!=null && batchQueryText.length >0){
				if(mode == ''){
					sessionStorage.removeItem("executeBatch");
					$(SRC_FILE_NAME).val("");
					srcQueryTextEditor.setValue(batchQueryText);
					$(EXEC_BTN).click();
				}else{
					alert("An execute is current in progress. Wait and come back to this page after it is complete.")
				}
			}
		},
		onFileSelected : function(e) {
			var files = e.target.files;
			var FILE = files[0];
			if (FILE) {
				$(SRC_FILE_NAME).val(FILE.name);
				var r = new FileReader();
				r.onload = function(e) { 
					var contents = e.target.result;
					srcQueryTextEditor.setValue(contents);
					$(EXEC_RESULTS).text("");
				}
				r.readAsText(FILE);
			} else { 
				alert("Failed to load file.");
			}
		},
		sessionTimeout: function() {
			window.location.hash = '/stimeout';
		},
		saveSQL: function(){
			var output = $(EXEC_RESULTS).text();
			if(output.length > 0) {
				var blob = new Blob([output], {type: "text/plain;charset=utf-8"});
				saveAs(blob, "ScriptExecute.log");
			}
		},
		executeBatchSQL: function(){
			if(mode == ''){
				var param = {};
				param.text = srcQueryTextEditor.getValue();
				param.timestamp = moment.utc().valueOf();
				execStartTime = param.timestamp;
				
				if(param.text != null && param.text.trim().length > 0){
					$(EXEC_RESULTS).text("Executing SQL ...");
					_this.setPageExecuteMode();
					serverHandler.executeBatchSQL(param);
				}else{
					alert("EsgynDB SQL text cannot be empty!");
				}
			}else{
				if( mode == 'execute'){
					mode == 'cancel';
					_this.cancelExecute();
				}
			}
		},
		cancelExecute: function(){
			var param = {};
			param.timestamp = execStartTime;
			serverHandler.cancelBatch(param);
		},
		batchExecSuccess: function(result){
			if(_this.redirectFlag){
				resultsAfterPause = true;
				lastExecuteResult = result;
				var msgObj={msg:'The script execution completed successfully.',tag:"success",url:_this.currentURL,shortMsg:"Script execute succeeded.",lastMessageOnly:true};
				common.fire(common.NOFITY_MESSAGE,msgObj);
				return;
			}
			_this.resePageExecuteMode();
			$(EXEC_RESULTS).text(result.output);
		},
		showErrorMessage: function (jqXHR) {
			if(_this.redirectFlag){
				resultsAfterPause = true;
				lastRawError = jqXHR;
				var msgObj={msg:'The script execution failed.',tag:"danger",url:_this.currentURL,shortMsg:"Script execute failed.",lastMessageOnly:true};
				common.fire(common.NOFITY_MESSAGE,msgObj);
				return;
			}
			_this.resePageExecuteMode();
			if(jqXHR.requestor !=null && jqXHR.requestor != _this) //error message is probably for different page
				return;
			$(EXEC_RESULTS).text(jqXHR.responseText);
		},
		setPageExecuteMode: function(){
			mode = 'execute';
			$(SRC_FILE_NAME).attr('disabled',true);
			$(SRC_FILE_SELECT).attr('disabled',true);
			srcQueryTextEditor.setOption("readOnly", true);
			$(EXEC_BTN).removeClass('btn-primary fa-play').addClass('btn-danger fa-stop');
			$(EXEC_BTN).text(" Cancel");
			$(CLEAR_BTN).attr('disabled', true);
		},
		resePageExecuteMode: function(){
			mode = '';
			_this.hideLoading();
			execStartTime = null;
			srcQueryTextEditor.setOption("readOnly", false);
			$(SRC_FILE_NAME).attr('disabled',false);
			$(SRC_FILE_SELECT).attr('disabled',false);
			$(EXEC_BTN).removeClass('btn-danger fa-stop').addClass('btn-primary fa-play');
			$(EXEC_BTN).text(" Execute");
			$(CLEAR_BTN).attr('disabled', false);
		}
	});

	return ScriptExecutorView;
});
