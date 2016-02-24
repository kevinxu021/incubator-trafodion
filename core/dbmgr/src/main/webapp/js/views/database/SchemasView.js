// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/db_schemas.html',
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
	var BREAD_CRUMB = '#database-crumb';
	var ERROR_CONTAINER = '#db-schemas-error-text',
		OBJECT_LIST_CONTAINER = '#db-schemas-list-container',
		REFRESH_ACTION = '#refreshAction';

	var SchemasView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			$(ERROR_CONTAINER).hide();
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			$(BREAD_CRUMB).hide();
			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			this.fetchSchemas();
		},
		doResume: function(args){
			$(BREAD_CRUMB).hide();
			$(REFRESH_ACTION).on('click', this.doRefresh);
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
		},
		doPause: function(){
			$(REFRESH_ACTION).off('click', this.doRefresh);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayObjectList);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_ERROR, this.showErrorMessage);
			$('a[data-toggle="pill"]').off('shown.bs.tab', this.selectFeature);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		doRefresh: function(){
			_this.fetchSchemas();
		},
		fetchSchemas: function(){
			_this.showLoading();
			dbHandler.fetchObjects('schemas', null);
		},
		updateBreadCrumbs: function(routeArgs){
			$(BREAD_CRUMB).empty();
			bCrumbsArray = [];
			bCrumbsArray.push({name: 'Schemas', link: '#/database'});
			$.each(bCrumbsArray, function(key, crumb){
				$(BREAD_CRUMB).append('<li><a href='+ crumb.link + '>'+crumb.name+'</a></li>')
			});
		},
		displayObjectList: function (result){
			_this.hideLoading();
			$(ERROR_CONTAINER).hide();
			var keys = result.columnNames;

			if(keys != null && keys.length > 0) {
				$(OBJECT_LIST_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-list-results"></table>';
				$(OBJECT_LIST_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];
				var link = result.parentLink != null ? result.parentLink : "";

				$.each(result.resultArray, function(i, data){
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
						oDataTable.clear().draw();
					}catch(Error){

					}
				}
				
				oDataTable = $('#db-object-list-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no schemas"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
<<<<<<< HEAD
					"bProcessing": true,
					paging: bPaging,
					"bAutoWidth": true,
=======
					processing: true,
					paging: bPaging,
					autoWidth: true,
>>>>>>> 6a8ca78bfeae73b51ba6a11c9b731d4e4127cd79
					"iDisplayLength" : 25, 
					"sPaginationType": "simple_numbers",
					"aaData": aaData, 
<<<<<<< HEAD
					//"aoColumns" : aoColumns,
					aoColumns : [
					             {"mData": 'Name', sClass: 'left', "sTitle": 'Name', 
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 var rowcontent = "<a href=\"#" + link + '?name=' + data ;
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
					             ],
				                 buttons: [
				                           'copy','csv','excel','pdf','print'
				                           ],					             
					             fnDrawCallback: function(){
					            	 $('#db-object-list-results td').css("white-space","nowrap");
					             }
=======
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ {
						"aTargets": [ 0 ],
						"mData": 0,
						"mRender": function ( data, type, full ) {
		            		 if(type == 'display') {
		            			 var rowcontent = "<a href=\"#" + link + '?name=' + data ;
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
						"className" : "dbmgr-nowrap",
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
						"className" : "dbmgr-nowrap",
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
						}
					},
					{
						"aTargets": [ 4 ],
						"mData": 4,
						"visible" : false,
						"searchable" : false
					}],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: 'Schemas' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: 'Schemas' }
	                           ],					             
		             fnDrawCallback: function(){
		            	 //$('#db-object-list-results td').css("white-space","nowrap");
		             }
>>>>>>> 6a8ca78bfeae73b51ba6a11c9b731d4e4127cd79
				});

				$('#db-object-list-results tbody').on( 'click', 'td', function (e, a) {
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
		},		
		showErrorMessage: function (jqXHR) {
			_this.hideLoading();
			$(OBJECT_LIST_CONTAINER).hide();
			$(ERROR_CONTAINER).show();
			
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
        		if(jqXHR.status != null && jqXHR.status == 0) {
        			$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
        		}
        	}
		}  
	});


	return SchemasView;
});
