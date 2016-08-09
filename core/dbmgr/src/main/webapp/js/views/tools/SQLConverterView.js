//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/sqlconvert.html',
        'jquery',
        'common',
        'handlers/ServerHandler',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'filesaver'
        ], function (BaseView, SQLConvertT, $, common, serverHandler, CodeMirror) {
	'use strict';

	var SPINNER = '#loadingImg';
	var SRC_FILE_NAME = "#source_file_name",
	SRC_FILE_SELECT = "#src_file",
	SRC_SQL_TYPE = '#src_sql_type',
	TGT_SQL_TYPE = '#tgt_sql_type',
	CLEAR_BTN = '#clearBtn',
	CONVERT_BTN = '#convertBtn',
	SAVE_BTN = '#saveBtn',
	EXEC_BTN = '#executeBtn'
	var resizeTimer = null;			

	var _this = null;
	var srcQueryTextEditor = null,
	tgtQueryTextEditor = null,
	isPaused = false;

	var SQLConverterView = BaseView.extend({

		template: _.template(SQLConvertT),

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
			$(CONVERT_BTN).on('click',this.convertSQL);
			$(SAVE_BTN).on('click', this.saveSQL);
			$(EXEC_BTN).on('click', this.executeBatchSQL);

			serverHandler.on(serverHandler.CONVERT_SQL_SUCCESS, this.displayResults);
			serverHandler.on(serverHandler.CONVERT_SQL_ERROR, this.showErrorMessage);
			
			if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
				common.defineEsgynSQLMime(CodeMirror);
			}

			srcQueryTextEditor = CodeMirror.fromTextArea(document.getElementById("source-query-text"), {
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
			$(srcQueryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"500px", "font-size":"12px"});

			tgtQueryTextEditor = CodeMirror.fromTextArea(document.getElementById("converted-query-text"), {
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
			$(tgtQueryTextEditor.getWrapperElement()).resizable({
				resize: function() {
					tgtQueryTextEditor.setSize($(this).width(), $(this).height());
				}
			});
			$(tgtQueryTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"675px", "font-size":"12px"});

			_this.clearAll();

		},
		doResume: function(){
			this.currentURL = window.location.hash;
			this.redirectFlag=false;
			isPaused = false;
			$(SRC_FILE_SELECT).on('change', this.onFileSelected);
			$(CLEAR_BTN).on('click',this.clearAll);
			$(CONVERT_BTN).on('click',this.convertSQL);
			$(SAVE_BTN).off('click', this.saveSQL);
			$(EXEC_BTN).on('click', this.executeBatchSQL);
			//serverHandler.on(serverHandler.CONVERT_SQL_SUCCESS, this.displayResults);
			//serverHandler.on(serverHandler.CONVERT_SQL_ERROR, this.showErrorMessage);
		},
		doPause:  function(){
			isPaused = true;
			$(SRC_FILE_SELECT).off('change', this.onFileSelected);
			$(CLEAR_BTN).off('click',this.clearAll);
			$(CONVERT_BTN).off('click',this.convertSQL);
			$(SAVE_BTN).off('click', this.saveSQL);
			$(EXEC_BTN).off('click', this.executeBatchSQL);
			//serverHandler.off(serverHandler.CONVERT_SQL_SUCCESS, this.displayResults);
			//serverHandler.off(serverHandler.CONVERT_SQL_ERROR, this.showErrorMessage);
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
			if(tgtQueryTextEditor)
				tgtQueryTextEditor.setValue("");
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
					tgtQueryTextEditor.setValue("");
					$(EXEC_RESULTS).text("");
				}
				r.readAsText(FILE);
			} else { 
				alert("Failed to load file.");
			}
		},
		convertSQL: function () {
			var param = {};
			param.text = srcQueryTextEditor.getValue();
			param.srcType = $(SRC_SQL_TYPE).val();
			param.tgtType = "esgyndb";//$(TGT_SQL_TYPE).val();
			if(param.text != null && param.text.length > 0){
				$(CONVERT_BTN).prop('disabled', true);
				$(EXEC_BTN).prop('disabled', true);
				serverHandler.convertSQL(param);
			}else{
				alert("Source SQL text cannot be empty!");
			}
		},

		sessionTimeout: function() {
			window.location.hash = '/stimeout';
		},

		displayResults: function (result){
			_this.hideLoading();
			tgtQueryTextEditor.setValue(result.convertedText);
			$(CONVERT_BTN).prop('disabled', false);
			$(EXEC_BTN).prop('disabled', false);
		},
		saveSQL: function(){
			var sqlText = tgtQueryTextEditor.getValue();
			if(sqlText.length > 0){
				var blob = new Blob([tgtQueryTextEditor.getValue()], {type: "text/plain;charset=utf-8"});
				saveAs(blob, "EsgynDBSQL.sql");
				var output = $(EXEC_RESULTS).text();
				if(output.length > 0) {
					var blob = new Blob([output], {type: "text/plain;charset=utf-8"});
					saveAs(blob, "EsgynDBSQL.log");
				}
			}
		},
		executeBatchSQL: function(){
			var sqlText = tgtQueryTextEditor.getValue();
			if(sqlText.length > 0){
				sessionStorage.setItem("executeBatch", sqlText);
				window.location.hash = '/tools/executescript';
			}
			
			/*var param = {};
			param.text = tgtQueryTextEditor.getValue();
			if(param.text != null && param.text.length > 0){
				$(EXEC_RESULTS).text("Executing SQL ...");
				$(CONVERT_BTN).prop('disabled', true);
				$(EXEC_BTN).prop('disabled', true);
				$('#executingImg').show();
				serverHandler.executeBatchSQL(param);
			}else{
				alert("EsgynDB SQL text cannot be empty!");
			}*/
		},
/*		batchExecSuccess: function(result){
			$('#executingImg').hide();
			$(EXEC_RESULTS).text(result.output);
			$(CONVERT_BTN).prop('disabled', false);
			$(EXEC_BTN).prop('disabled', false);
		},*/
		showErrorMessage: function (jqXHR) {
			$(CONVERT_BTN).prop('disabled', false);
			$(EXEC_BTN).prop('disabled', false);
			$('#executingImg').hide();
			if(jqXHR.requestor !=null && jqXHR.requestor != _this) //error message is probably for different page
				return;
			tgtQueryTextEditor.setValue(jqXHR.responseText);
		}        
	});

	return SQLConverterView;
});
