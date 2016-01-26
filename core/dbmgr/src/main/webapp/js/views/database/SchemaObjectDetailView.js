// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/db_schema_object_detail.html',
        'jquery',
        'handlers/DatabaseHandler',
        'common',
        '../../../bower_components/codemirror/lib/codemirror',
        '../../../bower_components/codemirror/mode/sql/sql',
        'jqueryui',
        'datatables',
        'datatablesBootStrap',
        'pdfmake'
        ], function (BaseView, DatabaseT, $, dbHandler, common, CodeMirror) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg';				
	var objColumnsDataTable = null,
		regionsDataTable = null,
		privilegesDataTable = null;
		
	var _this = null;
	var ddlTextEditor = null;
	
	var BREAD_CRUMB = '#database-crumb';
	var OBJECT_DETAILS_CONTAINER = '#object-details-container',
		ATTRIBUTES_CONTAINER = '#db-object-attributes-container',
		ERROR_CONTAINER = '#db-object-error-text',
		COLUMNS_CONTAINER = '#db-object-columns-container',
		REGIONS_CONTAINER = '#db-object-regions-container',
		DDL_CONTAINER = '#db-object-ddl-container',
		PRIVILEGES_CONTAINER = '#db-object-privileges-container',
		USAGES_CONTAINER = '#db-object-usages-container',
		OBJECT_NAME_CONTAINER = '#db-object-name',
		
		FEATURE_SELECTOR = '#db-object-feature-selector',
		ATTRIBUTES_SELECTOR = '#db-attributes-link',
		COLUMNS_SELECTOR = '#db-columns-link',
		REGIONS_SELECTOR = '#db-regions-link',
		DDL_SELECTOR = '#db-ddl-link',
		INDEXES_SELECTOR = '#db-indexes-link',
		PRIVILEGES_SELECTOR = '#db-privileges-link',
		USAGES_SELECTOR = '#db-usages-link',
		
		ATTRIBUTES_BTN = '#attributes-btn',
		DDL_BTN= '#ddl-btn',
		PRIVILEGES_BTN = '#privileges-btn',
		INDEXES_BTN = '#indexes-btn',
		COLUMNS_BTN = '#columns-btn',
		REGIONS_BTN = '#regions-btn',
		USAGES_BTN = '#usages-btn',

		REFRESH_ACTION = '#refreshAction';
	
	var routeArgs = null;
	var schemaName = null;
	var prevRouteArgs = null;
	var bCrumbsArray = [];
	var pageStatus = {};
	var objectAttributes = null;
	
	var SchemaObjectDetailView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			prevRouteArgs = args;
			pageStatus = {};
			
			schemaName = routeArgs.schema;
			objectAttributes = sessionStorage.getItem(routeArgs.name);
			if(objectAttributes != null){
				sessionStorage.removeItem(routeArgs.name);
				objectAttributes = JSON.parse(objectAttributes);
			}
			$(OBJECT_DETAILS_CONTAINER).hide();
			$(ERROR_CONTAINER).hide();
			
			if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
				common.defineEsgynSQLMime(CodeMirror);
			}

			ddlTextEditor = CodeMirror.fromTextArea(document.getElementById("object-ddl-text"), {
				mode: 'text/x-esgyndb',
				indentWithTabs: false,
				smartIndent: false,
				lineNumbers: false,
				lineWrapping: true,
				matchBrackets : true,
				autofocus: true,
				extraKeys: {"Ctrl-Space": "autocomplete"}
			});
			
			$(ddlTextEditor.getWrapperElement()).resizable({
				resize: function() {
					ddlTextEditor.setSize($(this).width(), $(this).height());
				}
			});
			//$(ddlTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"150px"});
			$(ddlTextEditor.getWrapperElement()).css({"border" : "1px solid #eee"});

			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);

			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.on(dbHandler.FETCH_COLUMNS_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.on(dbHandler.FETCH_REGIONS_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.showErrorMessage);
			_this.processRequest();

		},
		doResume: function(args){
			routeArgs = args;
			
			$(DDL_CONTAINER).hide();
			$(COLUMNS_CONTAINER).hide();

			$(REFRESH_ACTION).on('click', this.doRefresh);
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.on(dbHandler.FETCH_COLUMNS_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.on(dbHandler.FETCH_REGIONS_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.showErrorMessage);
			
			if(prevRouteArgs.schema != routeArgs.schema || 
					prevRouteArgs.name != routeArgs.name ||
					prevRouteArgs.type != routeArgs.type ){
				schemaName = routeArgs.schema;
				objectAttributes = sessionStorage.getItem(routeArgs.name);
				if(objectAttributes != null){
					sessionStorage.removeItem(routeArgs.name);
					objectAttributes = JSON.parse(objectAttributes);
				}
				if(objColumnsDataTable != null) {
					try {
						objColumnsDataTable.clear().draw();
					}catch(Error){

					}
				}
				if(regionsDataTable != null) {
					try {
						regionsDataTable.clear().draw();
					}catch(Error){

					}
				}
				if(privilegesDataTable != null) {
					try {
						privilegesDataTable.clear().draw();
					}catch(Error){
						var aa =4;
					}
				}
				pageStatus = {};
				$(ERROR_CONTAINER).hide();
				$(COLUMNS_CONTAINER).empty();
				$(REGIONS_CONTAINER).empty();
	        	if(ddlTextEditor){
	        		ddlTextEditor.setValue("");
	        		setTimeout(function() {
	        			ddlTextEditor.refresh();
	        		},1);
	        	}
			}	
			
			prevRouteArgs = args;
			var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
			var activeButton = null;
			if(ACTIVE_BTN){
				activeButton = '#'+ACTIVE_BTN.attr('id');
				if(activeButton == ATTRIBUTES_BTN || activeButton == DDL_BTN || activeButton == PRIVILEGES_BTN || activeButton == COLUMNS_BTN){
				}else{
					$(FEATURE_SELECTOR + ' a').first().tab('show')
				}
			}

			_this.processRequest();
		},
		doPause: function(){
			$(REFRESH_ACTION).off('click', this.doRefresh);
			dbHandler.off(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.off(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			dbHandler.off(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.off(dbHandler.FETCH_COLUMNS_ERROR, this.showErrorMessage);
			dbHandler.off(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.off(dbHandler.FETCH_REGIONS_ERROR, this.showErrorMessage);
			dbHandler.off(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.off(dbHandler.FETCH_PRIVILEGES_ERROR, this.showErrorMessage);
			dbHandler.off(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.off(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.showErrorMessage);
			
			$('a[data-toggle="pill"]').off('shown.bs.tab', this.selectFeature);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		getParentObjectName: function(){
			var parentObjectName = null;
			if(objectAttributes != null && objectAttributes.columns != null){
				$.each(objectAttributes.columns, function(index, value){
					for (var property in v) {
						if(property == 'Table Name'){
							parentObjectName = v[property];
							return;
						}
					}
				});
			}
			return parentObjectName;
		},
		getObjectID: function(){
			var objectID = null;
			if(objectAttributes != null){
				$.each(objectAttributes, function(index, v){
					for (var property in v) {
						if(property == 'Object ID'){
							objectID = v[property];
							return;
						}
					}
				});
			}
			return objectID;
		},
		selectFeature: function(e){
			$(OBJECT_DETAILS_CONTAINER).show();
			var selectedFeatureLink = ATTRIBUTES_SELECTOR;

			if(e && e.target && $(e.target).length > 0){
				selectedFeatureLink = $(e.target)[0].hash;
			}else{
				var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
				var activeButton = null;
				if(ACTIVE_BTN){
					activeButton = '#'+ACTIVE_BTN.attr('id');
				}
				switch(activeButton){
				case ATTRIBUTES_BTN:
					selectedFeatureLink = ATTRIBUTES_SELECTOR;
					break;
				case COLUMNS_BTN:
					selectedFeatureLink = COLUMNS_SELECTOR;
					break;
				case REGIONS_BTN:
					selectedFeatureLink = REGIONS_SELECTOR;
					break;
				case DDL_BTN:
					selectedFeatureLink = DDL_SELECTOR;
					break;
				case PRIVILEGES_BTN:
					selectedFeatureLink = PRIVILEGES_SELECTOR;
					break;
				case USAGES_BTN:
					selectedFeatureLink = USAGES_SELECTOR;
					break;
				case INDEXES_BTN:
					selectedFeatureLink = INDEXES_SELECTOR;
					break;				
				}
			}

			$(ATTRIBUTES_CONTAINER).hide();
			$(DDL_CONTAINER).hide();
			$(COLUMNS_CONTAINER).hide();
			$(REGIONS_CONTAINER).hide();
			
			switch(selectedFeatureLink){
			case ATTRIBUTES_SELECTOR:
				$(ATTRIBUTES_CONTAINER).show();
				_this.fetchAttributes();
				break;
			case COLUMNS_SELECTOR:
				$(COLUMNS_CONTAINER).show();
				_this.fetchColumns();
				break;
			case REGIONS_SELECTOR:
				$(REGIONS_CONTAINER).show();
				_this.fetchRegions();
				break;
			case DDL_SELECTOR:
				$(DDL_CONTAINER).show();
				_this.fetchDDLText();
				break;
			case PRIVILEGES_SELECTOR:
				$(PRIVILEGES_CONTAINER).show();
				_this.fetchPrivileges();
				break;
			case USAGES_SELECTOR:
				break;
			case INDEXES_SELECTOR:
				window.location.hash = '/database?type=indexes&schema='+schemaName;
				break;
			}
		},
		doRefresh: function(){
			var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
			var activeButton = null;
			if(ACTIVE_BTN){
				activeButton = '#'+ACTIVE_BTN.attr('id');
			}
			if(activeButton != null){
				switch(activeButton){
				case DDL_BTN:
					pageStatus.ddlFetched = false;
					break;
				case COLUMNS_BTN:
					pageStatus.columnsFetched = false;
					break;
				case REGIONS_BTN:
					pageStatus.regionsFetched = false;
					break;
				case PRIVILEGES_BTN:
					pageStatus.privilegesFetched = false;
					break;					
				}
			}
			_this.selectFeature();
			$(ERROR_CONTAINER).hide();
		},
		fetchDDLText: function(){
			if(!pageStatus.ddlFetched || pageStatus.ddlFetched == false ){
				_this.showLoading();
				var parentObjectName = null;
				if(routeArgs.type == 'index'){
					parentObjectName = _this.getParentObjectName();
				}
				dbHandler.fetchDDL(routeArgs.type, routeArgs.name, routeArgs.schema, parentObjectName);
			}
		},
		fetchColumns: function(){
			if(!pageStatus.columnsFetched || pageStatus.columnsFetched == false){
				_this.showLoading();
				dbHandler.fetchColumns(routeArgs.type, routeArgs.name, routeArgs.schema);
			}			
		},
		fetchRegions: function(){
			if(!pageStatus.regionsFetched || pageStatus.regionsFetched == false){
				_this.showLoading();
				dbHandler.fetchRegions(routeArgs.type, routeArgs.name, routeArgs.schema);
			}			
		},
		fetchPrivileges: function(){
			if(!pageStatus.privilegesFetched || pageStatus.privilegesFetched == false){
				_this.showLoading();
				var objectID = _this.getObjectID();
				dbHandler.fetchPrivileges(routeArgs.type, routeArgs.name, objectID, routeArgs.schema);
			}			
		},
		updateBreadCrumbs: function(routeArgs){
			$(BREAD_CRUMB).empty();
			bCrumbsArray = [];
			bCrumbsArray.push({name: 'Schemas', link: '#/database'});
			if(routeArgs.type != null && routeArgs.type.length > 0) {
				switch(routeArgs.type){
					case 'table': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Tables', link: '#/database/objects?type=tables&schema='+routeArgs.schema});
						bCrumbsArray.push({name: routeArgs.name, link: ''});
						break;
					case 'view': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Views', link: '#/database/objects?type=views&schema='+routeArgs.schema});
						bCrumbsArray.push({name: routeArgs.name, link: ''});
						break;
					case 'index': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Indexes', link: '#/database/objects?type=indexes&schema='+routeArgs.schema});
						bCrumbsArray.push({name: routeArgs.name, link: ''});
						break;
					case 'library': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Libraries', link: '#/database/objects?type=libraries&schema='+routeArgs.schema});
						bCrumbsArray.push({name: routeArgs.name, link: ''});
						break;
					case 'procedure': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'Procedures', link: '#/database/objects?type=procedures&schema='+routeArgs.schema});
						bCrumbsArray.push({name: routeArgs.name, link: ''});
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
			var displayName = common.toProperCase(routeArgs.type) + ' '+routeArgs.name;
			$(OBJECT_NAME_CONTAINER).text(displayName);

			$(BREAD_CRUMB).show();
			if(routeArgs.type != null && routeArgs.type.length > 0){
				switch(routeArgs.type){
					case 'table': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).show();
						$(REGIONS_BTN).show();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();
				
						_this.selectFeature();
						break;							
					case 'view': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).show();
						$(REGIONS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();				
						_this.selectFeature();
						break;
					case 'index': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).show();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).hide();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();					
						_this.selectFeature();
						break;
					case 'library': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();				
						_this.selectFeature();
						break;
					case 'procedure': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();				
						_this.selectFeature();
						break;							
				}
			}
		},
		fetchAttributes: function () {
			//var objectAttributes = sessionStorage.getItem(routeArgs.name);	
			if(objectAttributes == null){
				_this.hideLoading();
			}else{
				_this.hideLoading();
				//var properties = JSON.parse(objectAttributes);
				$(ATTRIBUTES_CONTAINER).empty();
				$(ATTRIBUTES_CONTAINER).append('<thead><tr><td style="width:200px;"><h2 style="color:black;font-size:15px;font-weight:bold">Name</h2></td><td><h2 style="color:black;font-size:15px;;font-weight:bold">Value</h2></td></tr></thead>');
				$.each(objectAttributes.columns, function(k, v){
					var property = v.title;
					var value = objectAttributes.data[k];
					if(property == 'CreateTime' || property == 'ModifiedTime'){
						value = common.toServerLocalDateFromUtcMilliSeconds(value);
					}
					$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
				});
				var parentObjectName = _this.getParentObjectName();
				if(parentObjectName != null){
					$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">Parent Object</td><td>' + parentObjectName +  '</td>');
				}
			}
		},
		fetchAttributes: function () {
			$(ERROR_CONTAINER).hide();
			if(objectAttributes == null){
				dbHandler.fetchAttributes(routeArgs.type, routeArgs.name, routeArgs.schema);
			}else{
				_this.displayAttributes();
			/*	_this.hideLoading();
				
				//var properties = JSON.parse(objectAttributes);
				$(ATTRIBUTES_CONTAINER).empty();
				$(ATTRIBUTES_CONTAINER).append('<thead><tr><td style="width:200px;"><h2 style="color:black;font-size:15px;font-weight:bold">Name</h2></td><td><h2 style="color:black;font-size:15px;;font-weight:bold">Value</h2></td></tr></thead>');
				$.each(objectAttributes, function(k, v){
					for (var property in v) {
						var value = v[property];
						if(property == 'CreateTime' || property == 'ModifiedTime'){
							value = common.toServerLocalDateFromUtcMilliSeconds(value);
						}
						$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
					}
				});*/
			}
		},
		displayAttributes: function(data) {
			_this.hideLoading();
			if(data != null){
				objectAttributes = data;
			}
			$(ATTRIBUTES_CONTAINER).empty();
			$(ATTRIBUTES_CONTAINER).append('<thead><tr><td style="width:200px;"><h2 style="color:black;font-size:15px;font-weight:bold">Name</h2></td><td><h2 style="color:black;font-size:15px;;font-weight:bold">Value</h2></td></tr></thead>');
			$.each(objectAttributes, function(k, v){
				for (var property in v) {
					var value = v[property];
					if(property == 'CreateTime' || property == 'ModifiedTime'){
						value = common.toServerLocalDateFromUtcMilliSeconds(value);
					}
					$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
				}
			});
		},		
		displayDDL: function(data){
			_this.hideLoading();
			pageStatus.ddlFetched = true;
			ddlTextEditor.setValue(data);
			ddlTextEditor.refresh();
		},
		displayColumns: function(result){
			_this.hideLoading();
			var keys = result.columnNames;
			$(ERROR_CONTAINER).hide();
			pageStatus.columnsFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(COLUMNS_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-columns-list"></table>';
				$(COLUMNS_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];
				var link = result.parentLink != null ? result.parentLink : "";

				$.each(result.resultArray, function(i, data){
					aaData.push(data);
				});

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var bPaging = aaData.length > 25;

				if(objColumnsDataTable != null) {
					try {
						objColumnsDataTable.clear().draw();
					}catch(Error){

					}
				}
				var sortColumn = 1;
				if(routeArgs.type == 'view'){
					sortColumn = 0;
				}
				objColumnsDataTable = $('#db-object-columns-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no columns"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"aoColumnDefs": [ {
						"aTargets": [ 0 ],
						"mData": 0,
						"mRender": function ( data, type, full ) {
		            		 if(type == 'display') {
		            			 if(routeArgs.type == 'table' && data == 'PRIMARY KEY')
		            				 return '<i class="fa fa-key" style="padding-left:15px"></i>';
		            			 else
		            				 return data;
		            		 }else { 
		            			 return data;
		            		 }
		            	 }
					}
					],
					"order": [[ sortColumn, "asc" ]],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Columns in "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Columns in "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-columns-list td').css("white-space","nowrap");
		             }
				});
			}
		},
		displayRegions: function(result){
			_this.hideLoading();
			var keys = result.columnNames;
			$(ERROR_CONTAINER).hide();
			pageStatus.regionsFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(REGIONS_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-regions-list"></table>';
				$(REGIONS_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];
				var link = result.parentLink != null ? result.parentLink : "";

				$.each(result.resultArray, function(i, data){
					aaData.push(data);
				});

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var bPaging = aaData.length > 25;

				if(regionsDataTable != null) {
					try {
						regionsDataTable.clear().draw();
					}catch(Error){

					}
				}
				regionsDataTable = $('#db-object-regions-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no regions"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					aAutoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"order": [[ 1, "asc" ]],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Regions for "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Regions for "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	 //$('#db-object-regions-list td').css("white-space","nowrap");
		             }
				});
			}
		},
		displayPrivileges: function(result){
			_this.hideLoading();
			var keys = result.columnNames;
			$(ERROR_CONTAINER).hide();
			pageStatus.privilegesFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(PRIVILEGES_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-privileges-list"></table>';
				$(PRIVILEGES_CONTAINER).html( sb );

				var aoColumns = [];
				var aaData = [];
				var link = result.parentLink != null ? result.parentLink : "";

				$.each(result.resultArray, function(i, data){
					aaData.push(data);
				});

				// add needed columns
				$.each(keys, function(k, v) {
					var obj = new Object();
					obj.title = v;
					aoColumns.push(obj);
				});

				var bPaging = aaData.length > 25;

				if(privilegesDataTable != null) {
					try {
						privilegesDataTable.clear().draw();
					}catch(Error){

					}
				}
				privilegesDataTable = $('#db-object-privileges-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no privileges"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					"order": [[ 1, "asc" ]],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Privileges for "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Privileges for "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-privileges-list td').css("white-space","nowrap");
		             }
				});
			}
		},		
		showErrorMessage: function (jqXHR) {
			_this.hideLoading();
			$(ERROR_CONTAINER).show();
			$(OBJECT_DETAILS_CONTAINER).hide();
			$(COLUMNS_CONTAINER).hide();
			$(REGIONS_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		}  
	});


	return SchemaObjectDetailView;
});
