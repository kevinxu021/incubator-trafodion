// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/db_schema_objects.html',
        'jquery',
        'handlers/DatabaseHandler',
        'common',
        'jqueryui',
        'datatables.net',
        'datatables.net-bs',
        'pdfmake'
        ], function (BaseView, DatabaseT, $, dbHandler, common) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg';			
	var oDataTable = null;
	var _this = null;
	var schemaName = null;
	var isPaused = false;
	
	var BREAD_CRUMB = '#database-crumb';
	var ERROR_CONTAINER = '#db-objects-error-text',
		OBJECT_LIST_CONTAINER = '#db-objects-list-container',
		OBJECT_NAME_CONTAINER = '#db-objects-name',
		CREATE_LIBRARY_CONTAINER = '#create-library-div',
		CREATE_LIBRARY_BUTTON = '#create-library-btn',
		REFRESH_ACTION = '#refreshAction';
	
	
	var routeArgs = null;
	var prevRouteArgs = null;
	
	var schemaName = null;
	var bCrumbsArray = [];
	var pageStatus = {};
	var refreshLibraries = false;
	
	var SchemaObjectsView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			prevRouteArgs = args;
			pageStatus = {};
			isPaused = false;
			schemaName = routeArgs.schema;
			
			$(ERROR_CONTAINER).hide();
			$(OBJECT_LIST_CONTAINER).hide();
			
			$(REFRESH_ACTION).on('click', this.doRefresh);
			$(CREATE_LIBRARY_BUTTON).on('click', this.createLibrary);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			common.on(common.LIBRARY_CREATED_EVENT, this.libraryCreatedEvent);
			common.on(common.LIBRARY_ALTERED_EVENT, this.libraryAlteredEvent);
			common.on(common.LIBRARY_DROPPED_EVENT, this.libraryDroppedEvent);

			_this.processRequest();
		},
		doResume: function(args){
			routeArgs = args;
			isPaused = false;
			$(ERROR_CONTAINER).hide();
			$(OBJECT_LIST_CONTAINER).hide();
			
			if(prevRouteArgs.schema != routeArgs.schema || 
				prevRouteArgs.type != routeArgs.type){
				schemaName = routeArgs.schema;
				_this.doReset();
			}else{
				if(routeArgs.type == 'libraries' && _this.refreshLibraries == true){
					_this.doReset();
					_this.refreshLibraries = false;
				}
			}
			prevRouteArgs = args;
			$(REFRESH_ACTION).on('click', this.doRefresh);
			$(CREATE_LIBRARY_BUTTON).on('click', this.createLibrary);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			_this.processRequest();
		},
		doPause: function(){
			isPaused = true;
			$(REFRESH_ACTION).off('click', this.doRefresh);
			$(CREATE_LIBRARY_BUTTON).off('click', this.createLibrary);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
		},
		doReset: function(){
			pageStatus = {};
			$(OBJECT_LIST_CONTAINER).empty();
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		doRefresh: function(){
			pageStatus[routeArgs.type] =  false;
			_this.processRequest();
			$(ERROR_CONTAINER).hide();
		},
		libraryCreatedEvent: function(){
			_this.reloadLibraries();
		},
		libraryAlteredEvent: function() {
			_this.reloadLibraries();			
		},
		libraryDroppedEvent: function(){
			_this.reloadLibraries();
		},
		reloadLibraries: function(){
			if(routeArgs.type == 'libraries'){
				if(!isPaused){
					_this.doRefresh();
				}else{
					_this.refreshLibraries = true;
				}
			}
		},
		createLibrary: function(){
			window.location.hash = '/tools/createlibrary?schema='+common.ExternalDisplayName(schemaName);
		},
		fetchObjects: function(objectType, schemaName){
			if(!pageStatus[objectType] || pageStatus[objectType] == false){
				_this.showLoading();
				dbHandler.fetchObjects(objectType, schemaName);
			}
		},
		updateBreadCrumbs: function(routeArgs){
			$(BREAD_CRUMB).empty();
			bCrumbsArray = [];
			bCrumbsArray.push({name: 'Schemas', link: '#/database'});
			if(routeArgs.type != null && routeArgs.type.length > 0) {
				switch(routeArgs.type){
					case 'tables': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Tables', link: ''});
						break;
					case 'views': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Views', link: ''});
						break;
					case 'indexes': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Indexes', link: ''});
						break;
					case 'libraries': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Libraries',  link: ''});
						break;
					case 'procedures': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Procedures', link:  ''});
						break;
					case 'functions': 
						bCrumbsArray.push({name: common.ExternalDisplayName(routeArgs.schema), link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Functions', link:  ''});
						break;
				}
			}
			$.each(bCrumbsArray, function(key, crumb){
				if(crumb.link && crumb.link.length >0){
					$(BREAD_CRUMB).append('<li><a href='+ crumb.link + '>'+crumb.name+'</a></li>')
				}else{
					$(BREAD_CRUMB).append('<li>'+crumb.name+'</li>')
				}
			});
		},
		processRequest: function(){
			_this.updateBreadCrumbs(routeArgs);

			$(BREAD_CRUMB).show();
			$(OBJECT_LIST_CONTAINER).show();
			if(routeArgs.type != null && routeArgs.type.length > 0){
				switch(routeArgs.type){
					case 'tables' :
					case 'views' :
					case 'indexes' :
					case 'libraries' :
					case 'procedures' :
						var displayName = common.toProperCase(routeArgs.type) + ' in schema ' + common.ExternalDisplayName(routeArgs.schema);
						$(OBJECT_NAME_CONTAINER).text(displayName);
						_this.fetchObjects(routeArgs.type, routeArgs.schema);
						break;
					case 'functions' :
						var displayName = 'Functions in schema ' + common.ExternalDisplayName(routeArgs.schema);
						$(OBJECT_NAME_CONTAINER).text(displayName);
						_this.fetchObjects(routeArgs.type, routeArgs.schema);
						break;
				}
				if(routeArgs.type == 'libraries'){
					$(CREATE_LIBRARY_CONTAINER).show();
				}else{
					$(CREATE_LIBRARY_CONTAINER).hide();
				}
			}
		},

		displayObjectList: function (result){
			if(result.objectType == routeArgs.type){
				_this.hideLoading();
				var keys = result.columnNames;
				$(ERROR_CONTAINER).hide();
				pageStatus[routeArgs.type] = true;
				
				if(keys != null && keys.length > 0) {
					$(OBJECT_LIST_CONTAINER).show();
					var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-objects-list-results"></table>';
					$(OBJECT_LIST_CONTAINER).html( sb );
	
					var aoColumns = [];
					var aaData = [];
					var link = result.parentLink != null ? result.parentLink : "";
	
					$.each(result.resultArray, function(i, data){
						//var rowData = {};
						//$.each(keys, function(k, v) {
						//	rowData[v] = data[k];
						//});
						/*aaData.push(
								{'Name' : data[0], 
									'Owner' : data[1],
									'CreateTime' : data[2],
									'ModifiedTime': data[3]
								});*/
						aaData.push(data);
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
							oDataTable.destroy();
						}catch(Error){
	
						}
					}
					
					var aoColumnDefs = [];
					aoColumnDefs.push({
							"aTargets": [ 0 ],
							"mData": 0,
							"mRender": function ( data, type, full ) {
			            		 if(type == 'display') {
			            			 var rowcontent = "<a href=\"#" + link + '&name=' + data ;
			            			 if(schemaName != null)
			            				 rowcontent += '&schema='+ schemaName;	            				 
	
			            			 rowcontent += "\">" + common.ExternalDisplayName(data) + "</a>";
			            			 return rowcontent;                         
			            		 }else { 
			            			 return data;
			            		 }
			            	 }
						});
					
					aoColumnDefs.push({
						"aTargets": [ 2 ],
						"mData": 2,
						"className" : "dbmgr-nowrap",
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
						}
					});
					aoColumnDefs.push({
						"aTargets": [ 3 ],
						"mData": 3,
						"className" : "dbmgr-nowrap",
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
						}
					});
					aoColumnDefs.push({
						"aTargets": [ 4 ],
						"mData": 4,
						"visible" : false,
						"searchable" : false
					});
					
					if(routeArgs.type == 'indexes'){
						aoColumnDefs.push({
							"aTargets": [ 5 ],
							"mData": 5,
							"mRender": function ( data, type, full ) {
			            		 if(type == 'display') {
			            			 var rowcontent = '<a href="#/database/objdetail?type=table&name=' + data ;
			            			 if(schemaName != null)
			            				 rowcontent += '&schema='+ routeArgs.schema;	            				 
	
			            			 rowcontent += '">' + common.ExternalAnsiName(routeArgs.schema,data) + '</a>';
			            			 return rowcontent;                         
			            		 }else { 
			            			 return data;
			            		 }
			            	 }
						});
					}
					if(routeArgs.type == 'procedures'){
						aoColumnDefs.push({
							"aTargets": [ 5 ],
							"mData": 5,
							"visible" : false,
							"searchable" : false
						});
						aoColumnDefs.push({
							"aTargets": [ 6 ],
							"mData": 6,
							"mRender": function ( data, type, full ) {
			            		 if(type == 'display') {
			            			 if(data != null && data.length > 0){
				            			 var libSchema = full[5];
				            			 var rowcontent = '<a href="#/database/objdetail?type=library&name=' + data ;
				            			 if(libSchema != null && libSchema.length > 0){
				            				 rowcontent += '&schema='+ libSchema;
				            				 rowcontent += '">' + common.ExternalAnsiName(libSchema,data) + '</a>';		            				 
				            			 }else{
				            				 rowcontent += '">' + common.ExternalAnsiName(libSchema,data) + '</a>';	
				            			 }
				            			 return rowcontent; 
			            			 }else{
			            				 return "";
			            			 }
			            		 }else { 
			            			 return data;
			            		 }
			            	 }
						});
					}
					if(routeArgs.type == 'functions'){
						aoColumnDefs.push({
							"aTargets": [ 7 ],
							"mData": 7,
							"visible" : false,
							"searchable" : false
						});
						aoColumnDefs.push({
							"aTargets": [ 8 ],
							"mData": 8,
							"mRender": function ( data, type, full ) {
			            		 if(type == 'display') {
			            			 if(data != null && data.length > 0){
				            			 var libSchema = full[7];
				            			 var rowcontent = '<a href="#/database/objdetail?type=library&name=' + data ;
				            			 if(libSchema != null && libSchema.length > 0){
				            				 rowcontent += '&schema='+ libSchema;
				            				 rowcontent += '">' + common.ExternalAnsiName(libSchema,data)  + '</a>';		            				 
				            			 }else{
				            				 rowcontent += '">' + common.ExternalAnsiName(libSchema,data) + '</a>';	
				            			 }
				            			 return rowcontent; 
			            			 }else{
			            				 return "";
			            			 }
			            		 }else { 
			            			 return data;
			            		 }
			            	 }
						});
					}				
					oDataTable = $('#db-objects-list-results').DataTable({
						"oLanguage": {
							"sEmptyTable": "There are no " + routeArgs.type
						},
						//dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
						dom: "<'row'<'col-md-8'lB><'col-md-4'f>>" +"<'row'<'col-md-12'<'datatable-scroll'tr>>><'row'<'col-md-12'ip>>",
						processing: true,
						paging: bPaging,
						autoWidth: true,
						"iDisplayLength" : 25, 
						"sPaginationType": "full_numbers",
						"aaData": aaData, 
						"aoColumns" : aoColumns,
						"aoColumnDefs": aoColumnDefs,
		                 buttons: [
		                           { extend : 'copy', exportOptions: { columns: ':visible' } },
		                           { extend : 'csv', exportOptions: { columns: ':visible' } },
		                           //{ extend : 'excel', exportOptions: { columns: ':visible' } },
		                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: $(OBJECT_NAME_CONTAINER).text(), orientation: 'landscape' },
		                           { extend : 'print', exportOptions: { columns: ':visible' }, title: $(OBJECT_NAME_CONTAINER).text() }
		                           ]
					});
	
	
					//$('#db-objects-list-results td').css("white-space","nowrap");
					$('#db-objects-list-results tbody').on( 'click', 'td', function (e, a) {
						if(oDataTable.cell(this)){
							var cell = oDataTable.cell(this).index();
							if(cell){
								if(cell.column == 0){
									var data = oDataTable.row(cell.row).data();
									if(data){
										var objAttributes = [];
										$.each(aoColumns, function(index, val){
											var attrib = {};
											attrib[val.title] = data[index];
											objAttributes.push(attrib);
										});
										sessionStorage.setItem(data[0], JSON.stringify(objAttributes));	
									}
								}
							}
						}
					});	
				}
			}
		},		
		showErrorMessage: function (jqXHR) {
			if(jqXHR.objectType == routeArgs.type){
				_this.hideLoading();
				$(ERROR_CONTAINER).show();
				$(OBJECT_LIST_CONTAINER).hide();
				if (jqXHR.responseText) {
					$(ERROR_CONTAINER).text(jqXHR.responseText);
				}else{
					if(jqXHR.status != null && jqXHR.status == 0) {
						$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
					}
				}
			}
		}  
	});


	return SchemaObjectsView;
});
