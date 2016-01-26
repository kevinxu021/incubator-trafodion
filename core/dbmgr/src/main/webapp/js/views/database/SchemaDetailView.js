//@@@ START COPYRIGHT @@@

//(C) Copyright 2015 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'views/BaseView',
        'text!templates/db_schema_detail.html',
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
	var oDataTable = null;
	var _this = null;
	var ddlTextEditor = null;

	var BREAD_CRUMB = '#database-crumb',
	SCHEMA_DETAILS_CONTAINER = '#schema-details-container',
	ATTRIBUTES_CONTAINER = '#db-object-attributes-container',
	DDL_CONTAINER = '#db-object-ddl-container',
	OBJECT_NAME_CONTAINER = '#db-object-name',
	ERROR_CONTAINER = '#db-object-error-text',
	PRIVILEGES_CONTAINER = '#db-object-privileges-container',
	FEATURE_SELECTOR  = '#db-object-feature-selector',
	ATTRIBUTES_SELECTOR = '#db-attributes-link',
	DDL_SELECTOR = '#db-ddl-link',
	PRIVILEGES_SELECTOR = '#db-privileges-link',
	TABLES_SELECTOR = '#db-tables-link',
	VIEWS_SELECTOR = '#db-views-link',
	INDEXES_SELECTOR = '#db-indexes-link',
	LIBRARIES_SELECTOR = '#db-libraries-link',
	PROCEDURES_SELECTOR = '#db-procedures-link',
	ATTRIBUTES_BTN = '#attributes-btn',
	DDL_BTN= '#ddl-btn',
	PRIVILEGES_BTN = '#privileges-btn',
	TABLES_BTN = '#tables-btn',
	VIEWS_BTN = '#views-btn',
	INDEXES_BTN = '#indexes-btn',
	LIBRARIES_BTN = '#libraries-btn',
	PROCEDURES_BTN = '#procedures-btn',
	REFRESH_ACTION = '#refreshAction';

	var routeArgs = null;
	var schemaName = null;
	var bCrumbsArray = [];
	var pageStatus = {};
	var objectAttributes = null;
	
	var SchemaDetailView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			schemaName = routeArgs.name;
			objectAttributes = sessionStorage.getItem(routeArgs.name);
			if(objectAttributes != null){
				sessionStorage.removeItem(routeArgs.name);
				objectAttributes = JSON.parse(objectAttributes);
			}
			$(SCHEMA_DETAILS_CONTAINER).hide();
			$(ERROR_CONTAINER).hide();
			if(CodeMirror.mimeModes["text/x-esgyndb"] == null){
				common.defineEsgynSQLMime(CodeMirror);
			}

			ddlTextEditor = CodeMirror.fromTextArea(document.getElementById("schema-ddl-text"), {
				mode: 'text/x-esgyndb',
				indentWithTabs: true,
				smartIndent: true,
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
			$(ddlTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"150px"});

			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);

			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.showErrorMessage);
			_this.processRequest();
		},
		doResume: function(args){
			routeArgs = args;
			$(REFRESH_ACTION).on('click', this.doRefresh);
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.showErrorMessage);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.showErrorMessage);
			
			if(schemaName != routeArgs.name){
				schemaName = routeArgs.name;
				objectAttributes = sessionStorage.getItem(routeArgs.name);
				if(objectAttributes != null){
					sessionStorage.removeItem(routeArgs.name);
					objectAttributes = JSON.parse(objectAttributes);
				}
				pageStatus = {};
	        	if(ddlTextEditor){
	        		ddlTextEditor.setValue("");
	        		setTimeout(function() {
	        			ddlTextEditor.refresh();
	        		},1);
	        	}
	        	if(oDataTable != null) {
					try {
						oDataTable.clear().draw();
					}catch(Error){

					}
				}
			}	
			var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
			var activeButton = null;
			if(ACTIVE_BTN){
				activeButton = '#'+ACTIVE_BTN.attr('id');
				if(activeButton == ATTRIBUTES_BTN || activeButton == DDL_BTN || activeButton == PRIVILEGES_BTN){
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
			$(LOADING_SELECTOR).hide();$('#db-object-feature-selector .active').attr('id')
		},
		selectFeature: function(e){
			$(SCHEMA_DETAILS_CONTAINER).show();
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
				case DDL_BTN:
					selectedFeatureLink = DDL_SELECTOR;
					break;
				case PRIVILEGES_BTN:
					selectedFeatureLink = PRIVILEGES_SELECTOR;
					break;
				case TABLES_BTN:
					selectedFeatureLink = TABLES_SELECTOR;
					break;
				case VIEWS_BTN:
					selectedFeatureLink = VIEWS_SELECTOR;
					break;
				case INDEXES_BTN:
					selectedFeatureLink = INDEXES_SELECTOR;
					break;
				case LIBRARIES_BTN:
					selectedFeatureLink = LIBRARIES_SELECTOR;
					break;
				case PROCEDURES_BTN:
					selectedFeatureLink = PROCEDURES_SELECTOR;
					break;
				}
			}

			$(ATTRIBUTES_CONTAINER).hide();
			$(DDL_CONTAINER).hide();

			switch(selectedFeatureLink){
			case ATTRIBUTES_SELECTOR:
				$(ATTRIBUTES_CONTAINER).show();
				_this.fetchAttributes();
				break;
			case DDL_SELECTOR:
				$(DDL_CONTAINER).show();
				_this.fetchDDLText();
				break;
			case PRIVILEGES_SELECTOR:
				$(PRIVILEGES_CONTAINER).show();
				_this.fetchPrivileges();
				break;
			case TABLES_SELECTOR:
				window.location.hash = '/database/objects?type=tables&schema='+schemaName;
				break;
			case VIEWS_SELECTOR:
				window.location.hash = '/database/objects?type=views&schema='+schemaName;
				break;
			case INDEXES_SELECTOR:
				window.location.hash = '/database/objects?type=indexes&schema='+schemaName;
				break;
			case LIBRARIES_SELECTOR:
				window.location.hash = '/database/objects?type=libraries&schema='+schemaName;
				break;
			case PROCEDURES_SELECTOR:
				window.location.hash = '/database/objects?type=procedures&schema='+schemaName;
				break;
			}
		},
		doRefresh: function(){
			pageStatus.ddl = false;
			_this.processRequest();
			$(ERROR_CONTAINER).hide();
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
		fetchDDLText: function(){
			if(!pageStatus.ddl || pageStatus.ddl == false){
				_this.showLoading();
				dbHandler.fetchDDL('schema', routeArgs.name, null);
			}
		},
		fetchPrivileges: function(){
			if(!pageStatus.privilegesFetched || pageStatus.privilegesFetched == false){
				_this.showLoading();
				var objectID = _this.getObjectID();
				dbHandler.fetchPrivileges('schema', routeArgs.name, objectID, null);
			}			
		},
		updateBreadCrumbs: function(routeArgs){
			$(BREAD_CRUMB).empty();
			bCrumbsArray = [];
			bCrumbsArray.push({name: 'Schemas', link: '#/database'});
			bCrumbsArray.push({name: routeArgs.name, link: ''});

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
			schemaName = routeArgs.name;
			var displayName = 'Schema ' + routeArgs.name;
			$(OBJECT_NAME_CONTAINER).text(displayName);
			$(ATTRIBUTES_BTN).show();
			$(ATTRIBUTES_SELECTOR).tab('show');
			//$(ATTRIBUTES_SELECTOR).trigger('click');
			//$('#db-object-feature-selector a:first').tab('show');
			$(DDL_BTN).show();
			$(PRIVILEGES_BTN).show();
			$(TABLES_BTN).show();
			$(VIEWS_BTN).show();
			$(INDEXES_BTN).show();
			$(LIBRARIES_BTN).show();
			$(PROCEDURES_BTN).show();					
			_this.selectFeature();
		},
		fetchAttributes: function () {
			$(ERROR_CONTAINER).hide();
			if(objectAttributes == null){
				dbHandler.fetchAttributes('schema', routeArgs.name);
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
			pageStatus.ddl = true;
			_this.hideLoading();
			ddlTextEditor.setValue(data);
			ddlTextEditor.refresh();
		},
		displayPrivileges: function(result){
			_this.hideLoading();
			var keys = result.columnNames;
			$(ERROR_CONTAINER).hide();
			pageStatus.privilegesFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(PRIVILEGES_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-schema-privileges-list"></table>';
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

				if(oDataTable != null) {
					try {
						oDataTable.clear().draw();
					}catch(Error){

					}
				}
				oDataTable = $('#db-schema-privileges-list').DataTable({
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
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Schema level privilges for " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Schema level privilges for " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-schema-privileges-list td').css("white-space","nowrap");
		             }
				});
			}
		},	
		showErrorMessage: function (jqXHR) {
			_this.hideLoading();
			$(ERROR_CONTAINER).show();
			$(SCHEMA_DETAILS_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		}  
	});


	return SchemaDetailView;
});
