//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/scriptexecute.html',
        'jquery',
        'common',
        'handlers/ServerHandler',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'filesaver'
        ], function (BaseView, ScriptExecutorT, $, common, serverHandler, CodeMirror) {
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
	isPaused = false;
	
	var ScriptExecutorView = BaseView.extend({

		template: _.template(ScriptExecutorT),

		showLoading: function(){
			$(SPINNER).show();
		},

		hideLoading: function () {
			$(SPINNER).hide();
		},
		doInit: function () {
			_this = this;
			this.currentURL = window.location.hash;
			this.hideLoading();
			
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

			},
		doResume: function(){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			isPaused = false;
			$(SRC_FILE_SELECT).on('change', this.onFileSelected);
			$(CLEAR_BTN).on('click',this.clearAll);
			$(SAVE_BTN).off('click', this.saveSQL);
			$(EXEC_BTN).on('click', this.executeBatchSQL);
		},
		doPause:  function(){
			isPaused = true;
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
				saveAs(blob, "EsgynDBSQL.log");
			}
		},
		executeBatchSQL: function(){
			var param = {};
			param.text = srcQueryTextEditor.getValue();
			if(param.text != null && param.text.length > 0){
				$(EXEC_RESULTS).text("Executing SQL ...");
				$(EXEC_BTN).prop('disabled', true);
				_this.showLoading();
				serverHandler.executeBatchSQL(param);
			}else{
				alert("EsgynDB SQL text cannot be empty!");
			}
		},
		batchExecSuccess: function(result){
			_this.hideLoading();
			$(EXEC_RESULTS).text(result.output);
			$(EXEC_BTN).prop('disabled', false);
		},
		showErrorMessage: function (jqXHR) {
			$(EXEC_BTN).prop('disabled', false);
			_this.hideLoading();
			if(jqXHR.requestor !=null && jqXHR.requestor != _this) //error message is probably for different page
				return;
			$(EXEC_RESULTS).text(jqXHR.responseText);
		}        
	});

	return ScriptExecutorView;
});
