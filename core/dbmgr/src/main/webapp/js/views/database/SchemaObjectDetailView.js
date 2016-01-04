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
        'jstree'
        ], function (BaseView, DatabaseT, $, dbHandler, common, CodeMirror) {
	'use strict';
	var LOADING_SELECTOR = '#loadingImg';				
	var oDataTable = null;
	var _this = null;
	var initialized = false;
	var ddlTextEditor = null;
	
	var BREAD_CRUMB = '#database-crumb';
	var OBJECT_DETAILS_CONTAINER = '#object-details-container',
		ATTRIBUTES_CONTAINER = '#db-object-attributes-container',
		ERROR_CONTAINER = '#db-object-error-text',
		COLUMNS_CONTAINER = '#db-object-columns-container',
		PARTITIONS_CONTAINER = '#db-object-partitions-container',
		DDL_CONTAINER = '#db-object-ddl-container',
		PRIVILEGES_CONTAINER = '#db-object-privileges-container',
		USAGES_CONTAINER = '#db-object-usages-container',
		OBJECT_NAME_CONTAINER = '#db-object-name',
		
		FEATURE_SELECTOR = '#db-object-feature-selector',
		ATTRIBUTES_SELECTOR = '#db-attributes-link',
		COLUMNS_SELECTOR = '#db-columns-link',
		PARTITIONS_SELECTOR = '#db-partitions-link',
		DDL_SELECTOR = '#db-ddl-link',
		INDEXES_SELECTOR = '#db-indexes-link',
		PRIVILEGES_SELECTOR = '#db-privileges-link',
		USAGES_SELECTOR = '#db-usages-link',
		REFRESH_ACTION = '#refreshAction';
	
	var routeArgs = null;
	var schemaName = null;
	var prevRouteArgs = null;
	var bCrumbsArray = [];
	
	var SchemaObjectDetailView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			schemaName = routeArgs.schema;
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
			$(ddlTextEditor.getWrapperElement()).css({"border" : "1px solid #eee", "height":"150px"});

			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);

			$(REFRESH_ACTION).on('click', this.doRefresh);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			_this.processRequest();

		},
		doResume: function(args){
			routeArgs = args;
			
			$(REFRESH_ACTION).on('click', this.doRefresh);
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			
			if(schemaName != routeArgs.name){
				schemaName = routeArgs.name;
				initialized = false;
	        	if(ddlTextEditor)
	        		ddlTextEditor.setValue("");
			}	
			var TAB_LINK = $(OBJECT_DETAILS_CONTAINER+' .tab-pane.active');
			if(TAB_LINK){
				var selectedTab = '#'+TAB_LINK.attr('id');
				if(selectedTab == ATTRIBUTES_SELECTOR || selectedTab == DDL_TEXT_SELECTOR || selectedTab == PRIVILEGES_SELECTOR){
					//no-op
				}else{
					$(OBJECT_DETAILS_CONTAINER +' a:first').tab('show');
				}
			}
			_this.processRequest();
		},
		doPause: function(){
			$(REFRESH_ACTION).off('click', this.doRefresh);
			dbHandler.off(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.off(dbHandler.FETCH_DDL_ERROR, this.showErrorMessage);
			$('a[data-toggle="pill"]').off('shown.bs.tab', this.selectFeature);
		},
		showLoading: function(){
			$(LOADING_SELECTOR).show();
		},

		hideLoading: function () {
			$(LOADING_SELECTOR).hide();
		},
		selectFeature: function(e){
			$(OBJECT_DETAILS_CONTAINER).show();
			var selectedTab = ATTRIBUTES_SELECTOR;
			var TAB_LINK = $(OBJECT_DETAILS_CONTAINER+' .tab-pane.active');
			if(TAB_LINK){
				selectedTab = '#'+TAB_LINK.attr('id');
			}else{
				return;
			}

			if(e && e.target && $(e.target).length > 0){
				selectedTab = $(e.target)[0].hash;
			}

			$(ATTRIBUTES_CONTAINER).hide();
			$(DDL_CONTAINER).hide();
			
			switch(selectedTab){
			case ATTRIBUTES_SELECTOR:
				$(ATTRIBUTES_CONTAINER).show();
				_this.fetchAttributes();
				break;
			case COLUMNS_SELECTOR:
				$(COLUMNS_CONTAINER).show();
				break;
			case PARTITIONS_SELECTOR:
				$(PARTITIONS_CONTAINER).show();
				break;
			case DDL_SELECTOR:
				$(DDL_CONTAINER).show();
				var ddlText = ddlTextEditor.getValue();
				if(ddlText == null || ddlText.length == 0){
					_this.fetchDDLText();
				}
				break;
			case PRIVILEGES_SELECTOR:
				break;
			case USAGES_SELECTOR:
				break;
			case INDEXES_SELECTOR:
				window.location.hash = '/database?type=indexes&schema='+schemaName;
				break;
			}
		},
		doRefresh: function(){
			_this.processRequest();
			$(ERROR_CONTAINER).hide();
		},
		fetchDDLText: function(){
			if(!initialized){
				_this.showLoading();
				dbHandler.fetchDDL(routeArgs.type, routeArgs.name, routeArgs.schema);
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

			$(BREAD_CRUMB).show();
			if(routeArgs.type != null && routeArgs.type.length > 0){
				switch(routeArgs.type){
					case 'table': 
						schemaName = routeArgs.schema;
						var displayName = common.toProperCase(routeArgs.type) + ' ' + routeArgs.name;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						$(ATTRIBUTES_CONTAINER).show();
						$(ATTRIBUTES_SELECTOR).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_SELECTOR).show();
						$(PARTITIONS_SELECTOR).show();
						$(DDL_SELECTOR).show();
						$(PRIVILEGES_SELECTOR).show();
						$(USAGES_SELECTOR).show();
						$(INDEXES_SELECTOR).hide();
				
						_this.selectFeature();
						break;							
					case 'view': 
						schemaName = routeArgs.schema;
						var displayName = common.toProperCase(routeArgs.type) + ' ' + routeArgs.name;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						$(ATTRIBUTES_CONTAINER).show();
						$(ATTRIBUTES_SELECTOR).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_SELECTOR).show();
						$(PARTITIONS_SELECTOR).hide();
						$(DDL_SELECTOR).show();
						$(PRIVILEGES_SELECTOR).show();
						$(USAGES_SELECTOR).show();
						$(INDEXES_SELECTOR).hide();				
						_this.selectFeature();
						break;
					case 'index': 
						schemaName = routeArgs.schema;
						var displayName = common.toProperCase(routeArgs.type) + ' ' + routeArgs.name;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						$(ATTRIBUTES_CONTAINER).show();
						$(ATTRIBUTES_SELECTOR).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_SELECTOR).show();
						$(PARTITIONS_SELECTOR).show();
						$(DDL_SELECTOR).show();
						$(PRIVILEGES_SELECTOR).show();
						$(USAGES_SELECTOR).show();
						$(INDEXES_SELECTOR).hide();					
						_this.selectFeature();
						break;
					case 'library': 
						schemaName = routeArgs.schema;
						var displayName = common.toProperCase(routeArgs.type) + ' ' + routeArgs.name;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						$(ATTRIBUTES_CONTAINER).show();
						$(ATTRIBUTES_SELECTOR).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_SELECTOR).hide();
						$(PARTITIONS_SELECTOR).hide();
						$(DDL_SELECTOR).show();
						$(PRIVILEGES_SELECTOR).show();
						$(USAGES_SELECTOR).show();
						$(INDEXES_SELECTOR).hide();				
						_this.selectFeature();
						break;
					case 'procedure': 
						schemaName = routeArgs.schema;
						var displayName = common.toProperCase(routeArgs.type) + ' ' + routeArgs.name;
						$(OBJECT_NAME_CONTAINER).text(displayName);
						$(ATTRIBUTES_CONTAINER).show();
						$(ATTRIBUTES_SELECTOR).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_SELECTOR).hide();
						$(PARTITIONS_SELECTOR).hide();
						$(DDL_SELECTOR).show();
						$(PRIVILEGES_SELECTOR).show();
						$(USAGES_SELECTOR).show();
						$(INDEXES_SELECTOR).hide();				
						_this.selectFeature();
						break;							
				}
			}
		},
		fetchAttributes: function () {
			var attrs = sessionStorage.getItem(routeArgs.name);	
			if(attrs == null){
				
			}else{
				_this.hideLoading();
				var properties = JSON.parse(attrs);
				$(ATTRIBUTES_CONTAINER).empty();
				$(ATTRIBUTES_CONTAINER).append('<thead><tr><td style="width:200px;"><h2 style="color:black;font-size:15px;font-weight:bold">Name</h2></td><td><h2 style="color:black;font-size:15px;;font-weight:bold">Value</h2></td></tr></thead>');
				for (var property in properties) {
					if(properties.hasOwnProperty(property)){
						var value = properties[property];
						if(property == 'CreateTime' || property == 'ModifiedTime'){
							value = common.toServerLocalDateFromUtcMilliSeconds(value);
						}
					}
					$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
				}
			}
		},
		displayDDL: function(data){
			_this.hideLoading();
			ddlTextEditor.setValue(data);
		},
		showErrorMessage: function (jqXHR) {
			_this.hideLoading();
			$(ERROR_CONTAINER).show();
			$(OBJECT_DETAILS_CONTAINER).hide();
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
