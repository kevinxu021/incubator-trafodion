// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/database.html',
        'jquery',
        'handlers/DatabaseHandler',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'tabletools',
        'jstree'
        ], function (BaseView, DatabaseT, $, dbHandler) {
	'use strict';
	var LOADING_SELECTOR = ".dbmgr-spinner";			
	var oDataTable = null;
	var _that = null;
	var _jsTree = null;
	var breadcrumb = '#database-crumb';
	var dataTableContainer = '#tabular-result-container',
		errorTextContainer = '#errorText',
		textResultContainer = '#text-result-container';
	var DB_TYPE_SCHEMA = 'S',
	DB_TYPE_TABLE = 'T',
	DB_TYPE_VIEW = 'V',
	DB_TYPE_INDEX = 'I',
	DB_TYPE_LIBRARY = 'L',
	DB_TYPE_PROCEDURE = 'R';

	var currentUrlFragment = null;
	
	var DatabaseView = BaseView.extend({
		template:  _.template(DatabaseT),

		init: function (urlFragment){
			_that = this;
			currentUrlFragment = urlFragment;
			$(dataTableContainer).hide();
			$(errorTextContainer).hide();
			$(textResultContainer).hide();
			
			if(urlFragment != null && urlFragment.length > 0){
				$(breadcrumb).append('<li>Test</li>');
			}
			this.processRequest();

			$("#refreshAction").on('click', this.doRefresh);
			dbHandler.on('fetchSchemasSuccess', this.displaySchemas);
			dbHandler.on('fetchSchemasError', this.showErrorMessage);
		},
		resume: function(urlFragment){
			currentUrlFragment = urlFragment;
			$(dataTableContainer).hide();
			$(errorTextContainer).hide();
			$(textResultContainer).hide();
			
			$("#refreshAction").on('click', this.doRefresh);
			if(urlFragment != null && urlFragment.length > 0){
				$(breadcrumb).append('<li>Test</li>');
			}
			dbHandler.on('fetchSchemasSuccess', this.displaySchemas);
			dbHandler.on('fetchSchemasError', this.showErrorMessage);
			_that.processRequest();
		},
		pause: function(){
			$("#refreshAction").off('click', this.doRefresh);
			dbHandler.off('fetchSchemasSuccess', this.displaySchemas);
			dbHandler.off('fetchSchemasError', this.showErrorMessage);
		},
		showLoading: function(){
			$('#loadingImg').show();
		},

		hideLoading: function () {
			$('#loadingImg').hide();
		},
		doRefresh: function(){
			_that.processRequest();
			$(errorTextContainer).hide();
		},
		fetchSchemas: function(){
			//this.fetchObjects('resources/db/schemas')
			dbHandler.fetchSchemas('resources/db/schemas');
		},
		processRequest: function(){
			if(currentUrlFragment == null || currentUrlFragment.length ==0) {
				//this.fetchObjects('resources/db/schemas');
				dbHandler.fetchSchemas('resources/db/schemas');
			}else{
				var params = currentUrlFragment.split("/");
				if(params != null && params.length > 0){
					switch(params[0]){
					case 'schema': 
						this.fetchSchema('resources/db/schema/' + params[1]);
						break;
					case 'tables' :
						this.fetchObjects('resources/db/tables');
					}
				}
			}
		},
		fetchSchema: function (uri) {
			_that.showLoading();

			$.ajax({
				url: uri,
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displaySchema,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},
		fetchObjects: function (uri) {
			_that.showLoading();

			$.ajax({
				url: uri,
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:_that.displayObjectDetails,
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},
		fetchTables: function (node) {
			_that.showLoading();

			$.ajax({
				url:'resources/db/tables?schema='+node.data.parent,
				type:'GET',
				dataType:"json",
				contentType: "application/json;",
				success:function(result) {
					//_that.displayTables(node, result);
					_that.displayObjectDetails(result);
				},
				error:function(jqXHR, res, error){
					_that.hideLoading();
					_that.showErrorMessage(jqXHR);
				}
			});
		},
		displaySchemas: function(result){
			_that.displayObjectDetails(result);
		},
		displaySchema: function(result){
			_that.displayObjectDetails(result);
		},
		displayObjectDetails: function (result){
			_that.hideLoading();
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				$(dataTableContainer).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="query-results"></table>';
				$(dataTableContainer).html( sb );

				var aoColumns = [];
				var aaData = [];
				var link = result.parentLink != null ? result.parentLink : "";

				$.each(result.resultArray, function(i, data){
					aaData.push(
							{'Name' : data[0], 
								'Owner' : data[1],
								'CreateTime' : data[2],
								'ModifiedTime': data[3]
							});
				});

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var bPaging = aaData.length > 25;

				if(oDataTable != null) {
					try {
						oDataTable.fnDestroy();
					}catch(Error){

					}
				}
				oDataTable = $('#query-results').dataTable({
					dom: 'T<"clear">lfrtip',
					"bProcessing": true,
					"bPaginate" : true, 
					"bAutoWidth": true,
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
					"aaData": aaData, 
					//"aoColumns" : aoColumns,
					aoColumns : [
					             {"mData": 'Name', sClass: 'left', "sTitle": 'Name', 
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 var rowcontent = "<a href=\"#" + link + '/' + data + "\">" + data + "</a>";
					            			 return rowcontent;                         
					            		 }else { 
					            			 return data;
					            		 }
					            	 }			        
					             },
					             {"mData": 'Owner', sClass: 'left', "sTitle": 'Owner'},	
					             {"mData": 'CreateTime', sClass: 'left', "sTitle": 'Create Time',
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 return new Date(data);                         
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             },			   
					             {"mData": 'ModifiedTime', sClass: 'left', "sTitle": 'Modified Time',
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 return new Date(data);                         
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             }
					             ],
					             paging: true,
					             "tableTools": {
					            	 "sSwfPath": "bower_components/datatables-tabletools/swf/copy_csv_xls_pdf.swf"
					             },
					             fnDrawCallback: function(){
					            	 $('#query-results td').css("white-space","nowrap");
					             }
				});


				$('#query-results td').css("white-space","nowrap");
			}

		},
		showErrorMessage: function (jqXHR) {
			_that.hideLoading();
			$(errorTextContainer).show();
			if (jqXHR.responseText) {
				$("#errorText").text(jqXHR.responseText);
			}
		}  
	});


	return DatabaseView;
});
