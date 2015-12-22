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
        'datatables',
        'datatablesBootStrap'
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
				
				oDataTable = $('#db-object-list-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no schemas"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
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
					            			 return common.toDateFromMilliSeconds(data);                          
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             },			   
					             {"mData": 'ModifiedTime', sClass: 'left', "sTitle": 'Modified Time',
					            	 "mRender": function ( data, type, full ) {
					            		 if(type == 'display') {
					            			 return common.toDateFromMilliSeconds(data);                        
					            		 }else { 
					            			 return data;
					            		 }
					            	 }
					             }
					             ],
					             paging: true,
				                 buttons: [
				                           'copy','csv','excel','pdf','print'
				                           ],					             
					             fnDrawCallback: function(){
					            	 $('#db-object-list-results td').css("white-space","nowrap");
					             }
				});


				$('#db-object-list-results td').css("white-space","nowrap");
				$('#db-object-list-results tbody').on( 'click', 'tr', function (e, a) {
					var data = oDataTable.row(this).data();
					if(data){
						sessionStorage.setItem(data['Name'], JSON.stringify(data));	
					}
				} );				
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
