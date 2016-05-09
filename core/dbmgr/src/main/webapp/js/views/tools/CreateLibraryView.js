//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([ 'views/BaseView', 'text!templates/create_library.html', 'jquery',
		'handlers/ToolsHandler', 'moment', 'common', 'views/RefreshTimerView',
		'jqueryui', 'datatables.net', 'datatables.net-bs', 'jqueryvalidate',
        'bootstrapNotify'
 ], function(BaseView,
		CreateLibraryT, $, tHandler, moment, common, refreshTimer) {
	'use strict';

	var _this = null;
	var PAGE_MODE = "NORMAL"; 
	var LIB_FORM = "#create-library-form";
	var SCHEMA_NAME = "#schema_name";
	var LIBRARY_NAME = "#library_name";
	var LIBRARY_ERROR = "#library_name_error";
	var FILE_NAME = "#file_name";
	var FILE_SELECT = "#file";
	var CREATE_BTN = "#create_btn";
	var CLEAR_BTN = "#clear_btn";
	var OVERWRITE_CHECKBOX = "#overwrite";
	var LOADING = "#loading-spinner";
	var PAGE_HEADER = "#create-library-page-header";
	var FILE = null;
	var CHUNKS = [];
	var UPLOAD_INDEX = 0;
	var UPLOAD_LENGTH = 0;
	var OVERWRITE_FLAG = false;
	var validator = null;
	var isAjaxCompleted=true;
	var _args = null;
	
	var CreateLibraryView = BaseView.extend({
		template : _.template(CreateLibraryT),
		currentLibraryName:null,
		currentSchemaName:null,

		doInit : function(args) {
			_this = this;
			_args = args;
			_this.processArgs();
			this.redirectFlag=false;
			this.currentURL = window.location.hash;
			$(CREATE_BTN).on('click', this.uploadFile);
			$(CLEAR_BTN).on('click', this.cleanField);
			$(FILE_SELECT).on('click', this.fileDialogOpened);
			$(FILE_SELECT).on('change', this.onFileSelected);

			tHandler.on(tHandler.CREATE_LIBRARY_ERROR, this.createLibraryError);
			tHandler.on(tHandler.CREATE_LIBRARY_SUCCESS, this.createLibrarySuccess);
			tHandler.on(tHandler.EXECUTE_UPLOAD_CHUNK, this.executeUploadChunk);
			$(LOADING).css('visibility', 'hidden');
			$(CREATE_BTN).prop('disabled', true);
			
			validator = $(LIB_FORM).validate({
				rules: {
					"schema_name": { required: true, validateSchemaName:true },
					"library_name": { required: true, validateLibraryName: true },
					"file_name": { required: true}
				},
				messages: {
					"schema_name": {"required":"Please enter a schema name"},
					"library_name": {"required":"Please enter a library name"},
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
			$.validator.addMethod("validateSchemaName", function(value, element) {
				var schemaName = $(SCHEMA_NAME).val();
				var isNormalName = (!schemaName.startsWith("_") && (schemaName.match("^[\"a-zA-Z0-9_]+$")!=null));
				if(!isNormalName){
					if(schemaName.startsWith('"') && schemaName.endsWith('"')){
						isNormalName = true;
					}
				}
				return isNormalName;

			}, "* Schema name contains special characters. It needs be enclosed within double quotes.");
			
			$.validator.addMethod("validateLibraryName", function(value, element) {
				var libName = $(LIBRARY_NAME).val();
				var isNormalName = (!libName.startsWith("_") && (libName.match("^[a-zA-Z0-9_]+$")!=null));
				if(!isNormalName){
					if(libName.startsWith('"') && libName.endsWith('"')){
						isNormalName = true;
					}
				}
				return isNormalName;

			}, "* Library name contains special characters. It needs be enclosed within double quotes.");
		},
		doResume : function(args) {
			$(FILE_SELECT).on('click', this.fileDialogOpened);
			this.currentURL = window.location.hash;
			_args = args;
			_this.processArgs();
			this.redirectFlag=false;
			if(this.isAjaxCompleted=true){
				$(LOADING).css('visibility', 'hidden');
				$(CREATE_BTN).prop('disabled', false);
				$(CLEAR_BTN).prop('disabled', false);
			}
		},
		doPause : function() {
			$(FILE_SELECT).off('click', this.fileDialogOpened);
			this.redirectFlag=true;
		},
		fileDialogOpened: function(){
			this.value = null; //reset the value so the file can be selected even if the same file that was selected before
		},
		processArgs: function(){
			if( _args.schema != undefined){
				$(SCHEMA_NAME).val( _args.schema);
				$(LIBRARY_NAME).val("");
				$(FILE_NAME).val("");
			}
			if(_args.library != undefined){
				$(LIBRARY_NAME).val(_args.library);
				$(LIBRARY_NAME).prop('disabled', true);
				$(SCHEMA_NAME).prop('disabled', true);
				$(PAGE_HEADER).text("Alter Library");
				$(CREATE_BTN).prop('value','Alter');
				$(OVERWRITE_CHECKBOX).prop('disabled', true);
				$(OVERWRITE_CHECKBOX).prop('checked' ,true);
				var libParams = sessionStorage.getItem(_args.library);
				sessionStorage.removeItem(_args.library);
				if(libParams != undefined){
					libParams = JSON.parse(libParams);
					if(libParams.file){
						$(FILE_NAME).val(libParams.file);
					}
				}
				PAGE_MODE = "UPDATE";
			}else{
				$(SCHEMA_NAME).prop('disabled', false);
				$(LIBRARY_NAME).prop('disabled', false);
				$(PAGE_HEADER).text("Create Library");
				$(CREATE_BTN).prop('value','Create');
				$(OVERWRITE_CHECKBOX).prop('disabled', false);
				$(OVERWRITE_CHECKBOX).prop('checked', false);
				PAGE_MODE = "CREATE";
			}
		},
		
		cleanField:function(){
			_this.processArgs();
			if(PAGE_MODE != "UPDATE"){
				$(SCHEMA_NAME).val("");
				$(LIBRARY_NAME).val("");
			}
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
			var schemaName = $(SCHEMA_NAME).val()==""?"_LIBMGR_": $(SCHEMA_NAME).val();
			var libraryName = $(LIBRARY_NAME).val();
			_this.currentLibraryName=$(LIBRARY_NAME).val();
			_this.currentSchemaName=schemaName;
			var chunk_size = 10000  * 1024; //1mb = 1 * 1024 * 1024;
			var file = FILE;
			var fileName = file.name;
			var filePart = 0;
			var fileSize = file.size;
			var start = 0;
			var end = chunk_size;
			var totalChunks = Math.ceil(fileSize / chunk_size);
			OVERWRITE_FLAG = false;
			UPLOAD_INDEX = 0;
			CHUNKS=[];
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
			OVERWRITE_FLAG = $(OVERWRITE_CHECKBOX).prop('checked');
			if(UPLOAD_LENGTH==1){
				_this.executeUploadChunk(OVERWRITE_FLAG, true, true);	
			}else{
				_this.executeUploadChunk(OVERWRITE_FLAG, true, false);
			}
			
		},
		
		executeUploadChunk : function(oflag, sflag, eflag, uflag){
			_this.isAjaxCompleted=false;
			var data = CHUNKS[UPLOAD_INDEX];
			var uflag = (PAGE_MODE == 'UPDATE');
			tHandler.createLibrary(data.chunk, data.fileName, data.filePart, data.fileSize, common.ExternalForm(data.schemaName), common.ExternalForm(data.libraryName), oflag, sflag, eflag, uflag);
			UPLOAD_INDEX++;
		}, 
		onFileSelected : function(e) {
			var files = e.target.files;
			FILE = files[0];
			$(CREATE_BTN).prop('disabled', false);
			$(FILE_NAME).val(FILE.name);
		},
		createLibrarySuccess : function(){
			_this.isAjaxCompleted=true;
			var msgPrefix = (PAGE_MODE == 'UPDATE' ? "altered" : "created");
			var msg= 'Library '+ common.ExternalForm(_this.currentSchemaName) + "." + common.ExternalForm(_this.currentLibraryName) + ' was ' + msgPrefix + ' successfully';
			if(UPLOAD_INDEX==UPLOAD_LENGTH){
				$(LOADING).css('visibility', 'hidden');
				$(CREATE_BTN).prop('disabled', false);
				$(CLEAR_BTN).prop('disabled', false);
				var msgObj={msg: msg,tag:"success",url:null,shortMsg:'Library was ' + msgPrefix + ' successfully.'};
				if(_this.redirectFlag==false){
					_this.popupNotificationMessage(null,msgObj);
				}else{
					
					common.fire(common.NOFITY_MESSAGE,msgObj);
				}
				common.fire(common.LIBRARY_CREATED_EVENT,'');
				
				//alert("Create library Success!");
			}else if(UPLOAD_INDEX==UPLOAD_LENGTH-1){
				_this.executeUploadChunk(OVERWRITE_FLAG, false, true);
			}else{
				_this.executeUploadChunk(OVERWRITE_FLAG, false, false);
			}
		},
		createLibraryError : function(error){
			_this.isAjaxCompleted=true;
			$(LOADING).css('visibility', 'hidden');
			$(CREATE_BTN).prop('disabled', false);
			$(CLEAR_BTN).prop('disabled', false);
			var errorIndex = error.responseText.lastIndexOf("*** ERROR");
			var errorString = error.responseText.substring(errorIndex);
			var msgPrefix = (PAGE_MODE == 'UPDATE' ? "Failed to alter library " : "Failed to create library ");
			var msg= msgPrefix + common.ExternalForm(_this.currentSchemaName) + "." + common.ExternalForm(_this.currentLibraryName)+ " : " + errorString;
			//alert(errorString);
			var msgObj={msg:msg,tag:"danger",url:null,shortMsg:msgPrefix};
			if(_this.redirectFlag==false){
				_this.popupNotificationMessage(null,msgObj);
			}else{
				
				common.fire(common.NOFITY_MESSAGE,msgObj);
			}
		}
	});

	return CreateLibraryView;
});
