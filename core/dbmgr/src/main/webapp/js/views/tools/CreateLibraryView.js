//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([ 'views/BaseView', 'text!templates/create_library.html', 'jquery',
		'handlers/ToolsHandler', 'moment', 'common', 'views/RefreshTimerView',
		'jqueryui', 'datatables', 'datatablesBootStrap', ], function(BaseView,
		CreateLibraryT, $, tHandler, moment, common, refreshTimer) {
	'use strict';

	var _this = null;
	var ERROR = "#error_mesg";
	var SCHEMA_NAME = "#shcema_name";
	var LIBRARY_NAME = "#library_name";
	var LIBRARY_ERROR = "#library_name_error";
	var FILE_NAME = "#file_name";
	var FILE_SELECT = "#file";
	var CREATE_BTN = "#create_btn"
	var PROGRESS_BAR = "#uploadprogress";
	var FILE = null;
	var CHUNKS = [];
	var UPLOAD_INDEX = 0;
	var UPLOAD_LENGTH = 0;

	var CreateLibraryView = BaseView.extend({
		template : _.template(CreateLibraryT),

		doInit : function(args) {
			_this = this;
			$(CREATE_BTN).on('click', this.uploadFile);
			$(FILE_SELECT).on('change', this.onFileSelected);
			tHandler.on(tHandler.UPDATE_PROGRESS, this.updateProgress);
			tHandler.on(tHandler.CREATE_LIBRARY_ERROR, this.createLibraryError);
			tHandler.on(tHandler.CREATE_LIBRARY_SUCCESS, this.createLibrarySuccess);
			tHandler.on(tHandler.EXECUTE_UPLOAD_CHUNK, this.executeUploadChunk);
		},
		doResume : function(args) {

		},
		doPause : function() {

		},
		updateProgress:function(percentComplete){
	        var value = document.getElementById('uploadprogress').value;
			var progress = value + percentComplete*100;
			var bar = document.getElementById('uploadprogress');
			bar.value = bar.innerHTML = progress;
		},
		cleanField:function(){
			$(SCHEMA_NAME).val="";
			$(LIBRARY_NAME).val="";
			$(FILE_NAME).val="";
			$(ERROR).hide();
			$(LIBRARY_ERROR).hide();
			var bar = document.getElementById('uploadprogress');
			bar.value = bar.innerHTML = 0;
			FILE=null;
			CHUNKS=[];
			UPLOAD_INDEX = 0;
			UPLOAD_LENGTH = 0;
		},
		uploadFile : function() {
			if($(LIBRARY_NAME).val()==""){
				$(LIBRARY_ERROR).show();
				return;
			}
			var bar = document.getElementById('uploadprogress');
			bar.value = bar.innerHTML = 0;
			var schemaName = $(SCHEMA_NAME).val()==""?"DB_LIBMGR": $(SCHEMA_NAME).val();
			var libraryName = $(LIBRARY_NAME).val();
			var chunk_size = 25  * 1024; //1mb = 1 * 1024 * 1024;
			var file = FILE;
			var fileName = file.name;
			var filePart = 0;
			var fileSize = file.size;
			var start = 0;
			var end = chunk_size;
			var totalChunks = Math.ceil(fileSize / chunk_size);
			
			while(start < fileSize){
				var slice_method = "";
				if ('mozSlice' in file) {
					slice_method = 'mozSlice';
				} else if ('webkitSlice' in file) {
					slice_method = 'webkitSlice';
				} else {
					slice_method = 'slice';
				}
				var chunk = file[slice_method](start, end);
				var data = {
						"chunk":chunk,
						"fileName":fileName,
						"filePart":filePart,
						"fileSize":fileSize,
						"schemaName":schemaName,
						"libraryName":libraryName
				}
				CHUNKS.push(data);
				filePart++;
				start = end;
				end = start + chunk_size;
			}
			UPLOAD_LENGTH = CHUNKS.length;
			_this.executeUploadChunk();
		},
		
		executeUploadChunk : function(flag){
			var data = CHUNKS[UPLOAD_INDEX];
			console.log(UPLOAD_INDEX +" has complete");
			tHandler.createLibrary(data.chunk, data.fileName, data.filePart, data.fileSize, data.schemaName, data.libraryName, flag);
			UPLOAD_INDEX++;
		}, 
		onFileSelected : function(e) {
			_this.cleanField();
			var files = e.target.files;
			FILE = files[0];
			$(CREATE_BTN).attr('disabled', false);
			$(FILE_NAME).val(FILE.name);
		},
		createLibrarySuccess : function(){
			var flag = false;
			if(UPLOAD_INDEX==UPLOAD_LENGTH){
				alert("create success");
			}else if(UPLOAD_INDEX==UPLOAD_LENGTH-1){
				flag = true;
			}else{
				_this.executeUploadChunk(flag);
			}
		},
		createLibraryError : function(){
			$(ERROR).show();
			$(ERROR).val("hello world")
		}
	});

	return CreateLibraryView;
});
