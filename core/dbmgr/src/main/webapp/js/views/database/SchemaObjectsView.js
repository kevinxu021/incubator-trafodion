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
        'datatables',
        'datatablesBootStrap',
        'jstree'
        ], function (BaseView, DatabaseT, $, dbHandler, common) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg';			
	var oDataTable = null;
	var _this = null;
	var schemaName = null;
	
	var BREAD_CRUMB = '#database-crumb';
	var ERROR_CONTAINER = '#db-objects-error-text',
		OBJECT_LIST_CONTAINER = '#db-objects-list-container',
		OBJECT_NAME_CONTAINER = '#db-objects-name',
		REFRESH_ACTION = '#refreshAction';
	
	
	var routeArgs = null;
	var prevRouteArgs = null;
	
	var schemaName = null;
	var bCrumbsArray = [];
	var pageStatus = {};
	
	var SchemaObjectsView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			prevRouteArgs = args;
			
			schemaName = routeArgs.schema;
			
			$(ERROR_CONTAINER).hide();
			$(OBJECT_LIST_CONTAINER).hide();
			
			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			_this.processRequest();
		},
		doResume: function(args){
			routeArgs = args;
			
			$(ERROR_CONTAINER).hide();
			$(OBJECT_LIST_CONTAINER).hide();
			
			if(prevRouteArgs.schema != routeArgs.schema || 
				prevRouteArgs.type != routeArgs.type){
				schemaName = routeArgs.schema;
				pageStatus = {};
				$(OBJECT_LIST_CONTAINER).empty();
			}
			prevRouteArgs = args;
			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			_this.processRequest();
		},
		doPause: function(){
			$(REFRESH_ACTION).off('click', this.doRefresh);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		doRefresh: function(){
			_this.processRequest();
			$(ERROR_CONTAINER).hide();
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
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Tables', link: ''});
						break;
					case 'views': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Views', link: ''});
						break;
					case 'indexes': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Indexes', link: ''});
						break;
					case 'libraries': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Libraries',  link: ''});
						break;
					case 'procedures': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Procedures', link:  ''});
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
						var displayName = common.toProperCase(routeArgs.type) + ' in schema ' + routeArgs.schema;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						_this.fetchObjects(routeArgs.type, routeArgs.schema);
						break;
				}
			}
		},

		displayObjectList: function (result){
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
						oDataTable.fnDestroy();
					}catch(Error){

					}
				}
				oDataTable = $('#db-objects-list-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no " + routeArgs.type
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					"bProcessing": true,
					paging: bPaging,
					"bAutoWidth": true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					//"scrollY":        "800px",
					"scrollCollapse": true,
					//"bJQueryUI": true,
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ {
						"aTargets": [ 0 ],
						"mData": 0,
						"mRender": function ( data, type, full ) {
		            		 if(type == 'display') {
		            			 var rowcontent = "<a href=\"#" + link + '&name=' + data ;
		            			 if(schemaName != null)
		            				 rowcontent += '&schema='+ schemaName;	            				 

		            			 rowcontent += "\">" + data + "</a>";
		            			 return rowcontent;                         
		            		 }else { 
		            			 return data;
		            		 }
		            	 }
					},
					{
						"aTargets": [ 2 ],
						"mData": 2,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
						}
					},
					{
						"aTargets": [ 3 ],
						"mData": 3,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
						}
					}
					],
					/*aoColumns : [
					             {"mData": 'Name', sClass: 'left', "sTitle": 'Name', 
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 var rowcontent = "<a href=\"#" + link + '&name=' + data ;
					            			 if(schemaName != null)
					            				 rowcontent += '&schema='+ schemaName;	            				 

					            			 rowcontent += "\">" + data + "</a>";
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
					            			 return common.toServerLocalDateFromUtcMilliSeconds(data);                          
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             },			   
					             {"mData": 'ModifiedTime', sClass: 'left', "sTitle": 'Modified Time',
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 return common.toServerLocalDateFromUtcMilliSeconds(data);                          
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             }
					             ],*/
				                 buttons: [
				                           'copy','csv','excel','pdf','print'
				                           ],					             
					             fnDrawCallback: function(){
					            	 $('#db-object-list-results td').css("white-space","nowrap");
					             }
				});


				$('#db-objects-list-results td').css("white-space","nowrap");
				$('#db-objects-list-results tbody').on( 'click', 'tr', function (e, a) {
					var data = oDataTable.row(this).data();
					if(data){
						//sessionStorage.setItem(data['Name'], JSON.stringify(data));
						var rowData = {};
						rowData.data = data;
						rowData.columns = aoColumns;
						sessionStorage.setItem(data[0], JSON.stringify(rowData));	
					}
				} );				
			}

		},		
		showErrorMessage: function (jqXHR) {
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
	});


	return SchemaObjectsView;
});
