//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([ 'views/BaseView', 'text!templates/create_library.html', 'jquery',
		'handlers/ToolsHandler', 'moment', 'common', 'views/RefreshTimerView',
		'jqueryui', 'datatables.net', 'datatables.net-bs', 'jqueryvalidate'
 ], function(BaseView,
		CreateLibraryT, $, tHandler, moment, common, refreshTimer) {
	'use strict';

	var _this = null;
	var LIB_FORM = "#create-library-form";
	var SCHEMA_NAME = "#schema_name";
	var LIBRARY_NAME = "#library_name";
	var LIBRARY_ERROR = "#library_name_error";
	var FILE_NAME = "#file_name";
	var FILE_SELECT = "#file";
	var CREATE_BTN = "#create_btn";
	var CLEAR_BTN = "#clear_btn";
	var LOADING = "#loading-spinner";
	var FILE = null;
	var CHUNKS = [];
	var UPLOAD_INDEX = 0;
	var UPLOAD_LENGTH = 0;
	var validator = null;
	
	var CreateLibraryView = BaseView.extend({
		template : _.template(CreateLibraryT),

		doInit : function(args) {
			_this = this;
			$(CREATE_BTN).on('click', this.uploadFile);
			$(CLEAR_BTN).on('click', this.cleanField);
			$(FILE_SELECT).on('change', this.onFileSelected);
			tHandler.on(tHandler.CREATE_LIBRARY_ERROR, this.createLibraryError);
			tHandler.on(tHandler.CREATE_LIBRARY_SUCCESS, this.createLibrarySuccess);
			tHandler.on(tHandler.EXECUTE_UPLOAD_CHUNK, this.executeUploadChunk);
			$(LOADING).css('visibility', 'hidden');
			$(CREATE_BTN).prop('disabled', true);
			
			validator = $(LIB_FORM).validate({
				rules: {
					"library_name": { required: true },
					"file_name": { required: true}
				},
				messages: {
					"library_name": "Please enter a library name",
					"file_name": "Please enter a code file name"
		        },
				highlight: function(element) {
					$(element).closest('.form-group').addClass('has-error');
				},
				unhighlight: function(element) {
					$(element).closest('.form-group').removeClass('has-error');
				},
				errorElement: 'span',
				errorClass: 'help-block',
				errorPlacement: function(error, element) {
					if(element.parent('.input-group').length) {
						error.insertAfter(element.parent());
					} else {
						error.insertAfter(element);
					}
				}
			});
		},
		doResume : function(args) {
			validator.resetForm();
		},
		doPause : function() {
			validator.resetForm();
		},
		
		cleanField:function(){
			$(SCHEMA_NAME).val("");
			$(LIBRARY_NAME).val("");
			$(FILE_NAME).val("");
			$(FILE_SELECT).val("");
			$(LIBRARY_ERROR).hide();
			$(LOADING).css('visibility', 'hidden');
			$(CREATE_BTN).prop('disabled', true);
			FILE=null;
			CHUNKS=[];
			UPLOAD_INDEX = 0;
			UPLOAD_LENGTH = 0;
		},
		uploadFile : function() {
			if($(LIB_FORM).valid()){

			}else{
				return;
			}
			
			if($(LIBRARY_NAME).val()==""){
				$(LIBRARY_ERROR).show();
				return;
			}
			$(LOADING).css('visibility', 'visible');
			$(CREATE_BTN).prop('disabled', true);
			$(CLEAR_BTN).prop('disabled', true);
			var schemaName = $(SCHEMA_NAME).val()==""?"DB__LIBMGR": $(SCHEMA_NAME).val();
			var libraryName = $(LIBRARY_NAME).val();
			var chunk_size = 10000  * 1024; //1mb = 1 * 1024 * 1024;
			var file = FILE;
			var fileName = file.name;
			var filePart = 0;
			var fileSize = file.size;
			var start = 0;
			var end = chunk_size;
			var totalChunks = Math.ceil(fileSize / chunk_size);
			UPLOAD_INDEX = 0;
			
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
			if(UPLOAD_LENGTH==1){
				_this.executeUploadChunk(true, true);	
			}else{
				_this.executeUploadChunk(true, false);
			}
			
		},
		
		executeUploadChunk : function(sflag, eflag){
			var data = CHUNKS[UPLOAD_INDEX];
			tHandler.createLibrary(data.chunk, data.fileName, data.filePart, data.fileSize, data.schemaName, data.libraryName, sflag, eflag);
			UPLOAD_INDEX++;
		}, 
		onFileSelected : function(e) {
			var files = e.target.files;
			FILE = files[0];
			$(CREATE_BTN).prop('disabled', false);
			$(FILE_NAME).val(FILE.name);
		},
		createLibrarySuccess : function(){
			if(UPLOAD_INDEX==UPLOAD_LENGTH){
				$(LOADING).css('visibility', 'hidden');
				$(CREATE_BTN).prop('disabled', false);
				$(CLEAR_BTN).prop('disabled', false);
				alert("Create library Success!");
			}else if(UPLOAD_INDEX==UPLOAD_LENGTH-1){
				_this.executeUploadChunk(false, true);
			}else{
				_this.executeUploadChunk(false, false);
			}
		},
		createLibraryError : function(error){
			$(LOADING).css('visibility', 'hidden');
			$(CREATE_BTN).prop('disabled', false);
			$(CLEAR_BTN).prop('disabled', false);
			var errorIndex = error.responseText.lastIndexOf("*** ERROR");
			var errorString = error.responseText.substring(errorIndex);
			alert(errorString);
		}
	});

	return CreateLibraryView;
});
