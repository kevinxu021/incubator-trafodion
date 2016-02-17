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
        'datatables.net',
        'datatables.net-bs',
        'pdfmake'
        ], function (BaseView, DatabaseT, $, dbHandler, common, CodeMirror) {
	'use strict';
	var ATTRIBUTES_SPINNER = '#attributes-spinner',
		COLUMNS_SPINNER = '#columns-spinner',
		REGIONS_SPINNER = '#regions-spinner',
		STATISTICS_SPINNER = '#statistics-spinner',
		DDL_SPINNER = '#ddl-spinner',
		PRIVILEGES_SPINNER = '#privileges-spinner',
		USAGES_SPINNER = '#usages-spinner',
		INDEXES_SPINNER = '#indexes-spinner';			
	var objColumnsDataTable = null,
		regionsDataTable = null,
		statisticsTable = null,
		privilegesDataTable = null,
		usagesDataTable = null,
		indexesDataTable = null;
		
	var _this = null;
	var ddlTextEditor = null;
	
	var BREAD_CRUMB = '#database-crumb';
	var OBJECT_DETAILS_CONTAINER = '#object-details-container',
		OBJECT_NAME_CONTAINER = '#db-object-name',
		ATTRIBUTES_CONTAINER = '#db-object-attributes-container',
		ATTRIBUTES_ERROR_CONTAINER = '#db-object-attributes-error-text',
		COLUMNS_CONTAINER = '#db-object-columns-container',
		COLUMNS_ERROR_CONTAINER = '#db-object-columns-error-text',
		REGIONS_CONTAINER = '#db-object-regions-container',
		REGIONS_ERROR_CONTAINER = '#db-object-regions-error-text',
		DDL_CONTAINER = '#db-object-ddl-container',
		DDL_ERROR_CONTAINER = '#db-object-ddl-error-text',
		PRIVILEGES_CONTAINER = '#db-object-privileges-container',
		PRIVILEGES_ERROR_CONTAINER = '#db-object-privileges-error-text',
		USAGES_CONTAINER = '#db-object-usages-container',
		USAGES_ERROR_CONTAINER = '#db-object-usages-error-text',
		STATISTICS_CONTAINER = '#db-object-statistics-container',
		STATISTICS_ERROR_CONTAINER = '#db-object-statistics-error-text',
		INDEXES_CONTAINER = '#db-object-indexes-container',
		INDEXES_ERROR_CONTAINER = '#db-object-indexes-error-text',
		
		FEATURE_SELECTOR = '#db-object-feature-selector',
		ATTRIBUTES_SELECTOR = '#db-attributes-link',
		COLUMNS_SELECTOR = '#db-columns-link',
		REGIONS_SELECTOR = '#db-regions-link',
		DDL_SELECTOR = '#db-ddl-link',
		INDEXES_SELECTOR = '#db-indexes-link',
		PRIVILEGES_SELECTOR = '#db-privileges-link',
		USAGES_SELECTOR = '#db-usages-link',
		STATISTICS_SELECTOR = '#db-statistics-link',
		
		ATTRIBUTES_BTN = '#attributes-btn',
		DDL_BTN= '#ddl-btn',
		PRIVILEGES_BTN = '#privileges-btn',
		INDEXES_BTN = '#indexes-btn',
		COLUMNS_BTN = '#columns-btn',
		REGIONS_BTN = '#regions-btn',
		USAGES_BTN = '#usages-btn',
		STATISTICS_BTN = '#statistics-btn',
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
				readOnly: true,
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
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.fetchDDLError);
			dbHandler.on(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.on(dbHandler.FETCH_COLUMNS_ERROR, this.fetchColumnsError);
			dbHandler.on(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.on(dbHandler.FETCH_REGIONS_ERROR, this.fetchRegionsError);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.fetchPrivilegesError);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.fetchAttributesError);
			dbHandler.on(dbHandler.FETCH_STATISTICS_SUCCESS, this.displayStatistics);
			dbHandler.on(dbHandler.FETCH_STATISTICS_ERROR, this.fetchStatisticsError);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayIndexes);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.fetchIndexesError);
			dbHandler.on(dbHandler.FETCH_USAGE_SUCCESS, this.displayUsages);
			dbHandler.on(dbHandler.FETCH_USAGE_ERROR, this.fetchUsagesError);
			_this.processRequest();

		},
		doResume: function(args){
			routeArgs = args;
			
			$(DDL_CONTAINER).hide();
			$(COLUMNS_CONTAINER).hide();

			$(REFRESH_ACTION).on('click', this.doRefresh);
			$('a[data-toggle="pill"]').on('shown.bs.tab', this.selectFeature);
			dbHandler.on(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.on(dbHandler.FETCH_DDL_ERROR, this.fetchDDLError);
			dbHandler.on(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.on(dbHandler.FETCH_COLUMNS_ERROR, this.fetchColumnsError);
			dbHandler.on(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.on(dbHandler.FETCH_REGIONS_ERROR, this.fetchRegionsError);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.on(dbHandler.FETCH_PRIVILEGES_ERROR, this.fetchPrivilegesError);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.on(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.fetchAttributesError);
			dbHandler.on(dbHandler.FETCH_STATISTICS_SUCCESS, this.displayStatistics);
			dbHandler.on(dbHandler.FETCH_STATISTICS_ERROR, this.fetchStatisticsError);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayIndexes);
			dbHandler.on(dbHandler.FETCH_OBJECT_LIST_ERROR, this.fetchIndexesError);
			dbHandler.on(dbHandler.FETCH_USAGE_SUCCESS, this.displayUsages);
			dbHandler.on(dbHandler.FETCH_USAGE_ERROR, this.fetchUsagesError);
			
			if(prevRouteArgs.schema != routeArgs.schema || 
					prevRouteArgs.name != routeArgs.name ||
					prevRouteArgs.type != routeArgs.type ){
				schemaName = routeArgs.schema;
				objectAttributes = sessionStorage.getItem(routeArgs.name);
				if(objectAttributes != null){
					sessionStorage.removeItem(routeArgs.name);
					objectAttributes = JSON.parse(objectAttributes);
				}
				_this.doReset();
			}	
			
			prevRouteArgs = args;
			
			_this.processRequest();
		},
		doPause: function(){
			$(REFRESH_ACTION).off('click', this.doRefresh);
			dbHandler.off(dbHandler.FETCH_DDL_SUCCESS, this.displayDDL);
			dbHandler.off(dbHandler.FETCH_DDL_ERROR, this.fetchDDLError);
			dbHandler.off(dbHandler.FETCH_COLUMNS_SUCCESS, this.displayColumns);
			dbHandler.off(dbHandler.FETCH_COLUMNS_ERROR, this.fetchColumnsError);
			dbHandler.off(dbHandler.FETCH_REGIONS_SUCCESS, this.displayRegions);
			dbHandler.off(dbHandler.FETCH_REGIONS_ERROR, this.fetchRegionsError);
			dbHandler.off(dbHandler.FETCH_PRIVILEGES_SUCCESS, this.displayPrivileges);
			dbHandler.off(dbHandler.FETCH_PRIVILEGES_ERROR, this.fetchPrivilegesError);
			dbHandler.off(dbHandler.FETCH_OBJECT_ATTRIBUTES_SUCCESS, this.displayAttributes);
			dbHandler.off(dbHandler.FETCH_OBJECT_ATTRIBUTES_ERROR, this.fetchAttributesError);
			dbHandler.off(dbHandler.FETCH_STATISTICS_SUCCESS, this.displayStatistics);
			dbHandler.off(dbHandler.FETCH_STATISTICS_ERROR, this.fetchStatisticsError);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_SUCCESS, this.displayIndexes);
			dbHandler.off(dbHandler.FETCH_OBJECT_LIST_ERROR, this.fetchIndexesError);
			dbHandler.off(dbHandler.FETCH_USAGE_SUCCESS, this.displayUsages);
			dbHandler.off(dbHandler.FETCH_USAGE_ERROR, this.fetchUsagesError);
			
			$('a[data-toggle="pill"]').off('shown.bs.tab', this.selectFeature);
		},
		doReset: function(){
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
				}
			}
			if(usagesDataTable != null) {
				try {
					usagesDataTable.clear().draw();
				}catch(Error){
				}
			}
			if(statisticsTable != null){
				try{
					statisticsTable.clear().draw();
				}catch(Error){
					
				}
			}
			if(indexesDataTable != null){
				try{
					indexesDataTable.clear.draw();
				}catch(Error){
					
				}
			}
			pageStatus = {};

			$(COLUMNS_CONTAINER).empty();
			$(REGIONS_CONTAINER).empty();
			$(STATISTICS_CONTAINER).empty();
			$(PRIVILEGES_CONTAINER).empty();
			$(STATISTICS_CONTAINER).empty();
			$(INDEXES_CONTAINER).empty();
			
        	if(ddlTextEditor){
        		ddlTextEditor.setValue("");
        		setTimeout(function() {
        			ddlTextEditor.refresh();
        		},1);
        	}			
		},

		getParentObjectName: function(){
			var parentObjectName = null;
			if(objectAttributes != null){
				$.each(objectAttributes, function(k, v){
					for (var property in v) {
						if(routeArgs.type == 'index' && property == 'Table Name'){
							parentObjectName = v[property];
							return;
						}
					}
				});
			}
			return parentObjectName;
		},
		
		getUsageSchemaName: function(){
			
			var usageSchemaName = null;
			if(objectAttributes != null){
				$.each(objectAttributes, function(k, v){
					for (var property in v) {
						if(property == 'UsageSchemaName'){
							usageSchemaName = v[property];
							return;
						}
					}
				});
			}
			return usageSchemaName;		
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
				case STATISTICS_BTN:
					selectedFeatureLink = STATISTICS_SELECTOR;
					break;
				}
			}

			$(ATTRIBUTES_CONTAINER).hide();
			$(DDL_CONTAINER).hide();
			$(COLUMNS_CONTAINER).hide();
			$(REGIONS_CONTAINER).hide();
			$(STATISTICS_CONTAINER).hide();
			$(USAGES_CONTAINER).hide();
			$(INDEXES_CONTAINER).hide();
			$(ATTRIBUTES_ERROR_CONTAINER).hide();
			$(DDL_ERROR_CONTAINER).hide();
			$(COLUMNS_ERROR_CONTAINER).hide();
			$(REGIONS_ERROR_CONTAINER).hide();
			$(STATISTICS_ERROR_CONTAINER).hide();
			$(USAGES_ERROR_CONTAINER).hide();
			$(INDEXES_ERROR_CONTAINER).hide();
			
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
			case STATISTICS_SELECTOR:
				$(STATISTICS_CONTAINER).show();
				_this.fetchStatistics();
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
				$(USAGES_CONTAINER).show();
				_this.fetchUsages();
				break;
			case INDEXES_SELECTOR:
				//window.location.hash = '/database?type=indexes&schema='+schemaName;
				$(INDEXES_CONTAINER).show();
				_this.fetchIndexes();
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
				case USAGES_BTN:
					pageStatus.usagesFetched = false;
					break;
				case STATISTICS_BTN:
					pageStatus.statisticsFetched = false;
					break;	
				case INDEXES_BTN:
					pageStatus.indexesFetched = false;
					break;						
				}
			}
			_this.selectFeature();
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
					case 'udf': 
						bCrumbsArray.push({name: routeArgs.schema, link: '#/database/schema?name='+routeArgs.schema});
						bCrumbsArray.push({name: 'User Defined Functions', link: '#/database/objects?type=udfs&schema='+routeArgs.schema});
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
						$(STATISTICS_BTN).show();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).show();
						break;							
					case 'view': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).show();
						$(REGIONS_BTN).hide();
						$(STATISTICS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();				
						break;
					case 'index': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).show();
						$(STATISTICS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).hide();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();					
						break;
					case 'library': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).hide();
						$(STATISTICS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).show();
						$(INDEXES_BTN).hide();				
						break;
					case 'procedure': 
					case 'udf': 
						schemaName = routeArgs.schema;
						$(ATTRIBUTES_BTN).show();
						$(ATTRIBUTES_SELECTOR).tab('show');
						$(COLUMNS_BTN).hide();
						$(REGIONS_BTN).hide();
						$(STATISTICS_BTN).hide();
						$(DDL_BTN).show();
						$(PRIVILEGES_BTN).show();
						$(USAGES_BTN).hide();
						$(INDEXES_BTN).hide();				
						break;							
				}
			}
			var ACTIVE_BTN = $(FEATURE_SELECTOR + ' .active');
			var activeButton = null;
			if(ACTIVE_BTN){
				activeButton = '#'+ACTIVE_BTN.attr('id');
				if($(activeButton).is(':visible') && (activeButton == ATTRIBUTES_BTN || activeButton == DDL_BTN || activeButton == PRIVILEGES_BTN
						|| activeButton == COLUMNS_BTN  || activeButton == USAGES_BTN)){
				}else{
					$(FEATURE_SELECTOR + ' a').first().tab('show')
				}
			}
			_this.selectFeature();

		},
		fetchAttributes: function () {
			$(ATTRIBUTES_ERROR_CONTAINER).hide();
			if(objectAttributes == null){
				$(ATTRIBUTES_SPINNER).show();
				dbHandler.fetchAttributes(routeArgs.type, routeArgs.name, routeArgs.schema);
			}else{
				_this.displayAttributes();
			}
		},
		fetchDDLText: function(){
			if(!pageStatus.ddlFetched || pageStatus.ddlFetched == false ){
				$(DDL_SPINNER).show();
				var parentObjectName = null;
				if(routeArgs.type == 'index'){
					parentObjectName = _this.getParentObjectName();
				}
				dbHandler.fetchDDL(routeArgs.type, routeArgs.name, routeArgs.schema, parentObjectName);
			}
		},
		fetchColumns: function(){
			if(!pageStatus.columnsFetched || pageStatus.columnsFetched == false){
				$(COLUMNS_SPINNER).show();
				dbHandler.fetchColumns(routeArgs.type, routeArgs.name, routeArgs.schema);
			}			
		},
		fetchRegions: function(){
			if(!pageStatus.regionsFetched || pageStatus.regionsFetched == false){
				$(COLUMNS_SPINNER).show();
				dbHandler.fetchRegions(routeArgs.type, routeArgs.name, routeArgs.schema);
			}			
		},
		fetchStatistics: function(){
			if(!pageStatus.statisticsFetched || pageStatus.statisticsFetched == false){
				$(STATISTICS_SPINNER).show();
				var objectID = _this.getObjectID();
				dbHandler.fetchStatistics(routeArgs.type, routeArgs.name, objectID, routeArgs.schema);
			}			
		},
		fetchPrivileges: function(){
			if(!pageStatus.privilegesFetched || pageStatus.privilegesFetched == false){
				$(PRIVILEGES_SPINNER).show();
				var objectID = _this.getObjectID();
				dbHandler.fetchPrivileges(routeArgs.type, routeArgs.name, objectID, routeArgs.schema);
			}			
		},
		fetchUsages: function(){
			if(!pageStatus.usagesFetched || pageStatus.usagesFetched == false){
				$(USAGES_SPINNER).show();
				var objectID = _this.getObjectID();
				dbHandler.fetchUsages(routeArgs.type, routeArgs.name, objectID, routeArgs.schema);
			}			
		},		
		fetchIndexes: function(){
			if(!pageStatus.indexesFetched || pageStatus.indexesFetched == false){
				$(INDEXES_SPINNER).show();
				dbHandler.fetchObjects("indexes", routeArgs.schema, routeArgs.name);
			}			
		},
		displayAttributes: function(data) {
			$(ATTRIBUTES_SPINNER).hide();
			if(data != null){
				objectAttributes = data;
			}
			$(ATTRIBUTES_ERROR_CONTAINER).hide();
			$(ATTRIBUTES_CONTAINER).empty();
			$(ATTRIBUTES_CONTAINER).append('<thead><tr><td style="width:200px;"><h2 style="color:black;font-size:15px;font-weight:bold">Name</h2></td><td><h2 style="color:black;font-size:15px;;font-weight:bold">Value</h2></td></tr></thead>');
			$.each(objectAttributes, function(k, v){
				for (var property in v) {
					var value = v[property];
					if(property == 'CreateTime' || property == 'ModifiedTime'){
						value = common.toServerLocalDateFromUtcMilliSeconds(value);
					}
					if(routeArgs.type == 'index' && property == 'Table Name'){
						var parentObjectName = _this.getParentObjectName();
						if(parentObjectName != null && parentObjectName.length >0) {
							var link =	'<a href="#/database/objdetail?type=table' 
						 		+ '&name=' + value 
					 			+ '&schema='+ routeArgs.schema            				 
	      			 			+ '">' + value + '</a>';
							$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + link +  '</td>');
							
						}else {
							$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
						}
					}else if((routeArgs.type == 'procedure' || routeArgs.type == 'udf') && property == 'UsageSchemaName'){
						continue;
					}else if((routeArgs.type == 'procedure' || routeArgs.type == 'udf') && property == 'Library Name'){
						var libSch = _this.getUsageSchemaName();
						libSch = (libSch != null && libSch.length > 0) ? libSch : routeArgs.schema;
						var link =	'<a href="#/database/objdetail?type=library&name=' + value + '&schema=' +  libSch           				 
      			 			+ '">' + libSch+'.'+value + '</a>';
						$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + link +  '</td>');
					}else{
						$(ATTRIBUTES_CONTAINER).append('<tr><td style="padding:3px 0px">' + property + '</td><td>' + value +  '</td>');
					}
				}
			});
		},		
		displayDDL: function(data){
			$(DDL_SPINNER).hide();
			$(DDL_ERROR_CONTAINER).hide();
			pageStatus.ddlFetched = true;
			ddlTextEditor.setValue(data);
			ddlTextEditor.refresh();
		},
		displayColumns: function(result){
			$(COLUMNS_SPINNER).hide();
			var keys = result.columnNames;
			$(COLUMNS_ERROR_CONTAINER).hide();
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
		            			 if(routeArgs.type == 'table' && data == 'P')
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
	                           { extend : 'copy', exportOptions: { columns: ':visible', orthogonal: 'export'  } },
	                           { extend : 'csv', exportOptions: { columns: ':visible', orthogonal: 'export' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible', orthogonal: 'export' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible', orthogonal: 'export'  }, title: "Columns in "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible', orthogonal: 'export' }, title: "Columns in "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-columns-list td').css("white-space","nowrap");
		             }
				});
			}
		},
		displayRegions: function(result){
			$(REGIONS_SPINNER).hide();
			var keys = result.columnNames;
			$(REGIONS_ERROR_CONTAINER).hide();
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
			$(PRIVILEGES_SPINNER).hide();
			var keys = result.columnNames;
			$(PRIVILEGES_ERROR_CONTAINER).hide();
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
		displayUsages: function(result){
			$(USAGES_SPINNER).hide();
			var keys = result.columnNames;
			$(USAGES_ERROR_CONTAINER).hide();
			pageStatus.usagesFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(USAGES_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-usage-list"></table>';
				$(USAGES_CONTAINER).html( sb );

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

				if(usagesDataTable != null) {
					try {
						usagesDataTable.clear().draw();
					}catch(Error){

					}
				}
				var aoColumnDefs = [];
				if(routeArgs.type == 'library'){
					aoColumnDefs.push({
						"aTargets": [ 1 ],
						"mData": 1,
						"visible" : false,
						"searchable" : false
					});
					aoColumnDefs.push({
						"aTargets": [ 0 ],
						"mData": 0,
						"mRender": function ( data, type, full ) {
		            		 if(type == 'display') {
		            			 if(data != null && data.length > 0){
			            			 var udrSchema = full[1];
			            			 var udrType = full[2];
			            			 var rtype = udrType != null && udrType == 'Procedure' ? 'procedure':'udf';
			            			 var rowcontent = '<a href="#/database/objdetail?type='+rtype+'&name=' + data ;
			            			 if(udrSchema != null && udrSchema.length > 0){
			            				 rowcontent += '&schema='+ udrSchema;
			            				 rowcontent += '">' + udrSchema+'.'+data + '</a>';		            				 
			            			 }else{
			            				 rowcontent += '">' + udrSchema+'.'+data + '</a>';	
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
				usagesDataTable = $('#db-object-usage-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no objects using this " + routeArgs.type
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					aoColumnDefs: aoColumnDefs,
					"order": [[ 1, "asc" ]],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Objects using "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Objects using "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-privileges-list td').css("white-space","nowrap");
		             }
				});
			}
		},	
		displayIndexes: function(result){
			$(INDEXES_SPINNER).hide();
			var keys = result.columnNames;
			$(INDEXES_ERROR_CONTAINER).hide();
			pageStatus[routeArgs.type] = true;
			
			if(keys != null && keys.length > 0) {
				$(INDEXES_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-objects-list-results"></table>';
				$(INDEXES_CONTAINER).html( sb );

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

				if(indexesDataTable != null) {
					try {
						indexesDataTable.clear().draw();
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

		            			 rowcontent += "\">" + data + "</a>";
		            			 return rowcontent;                         
		            		 }else { 
		            			 return data;
		            		 }
		            	 }
					});
				
				aoColumnDefs.push({
					"aTargets": [ 2 ],
					"mData": 2,
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
				
				aoColumnDefs.push({
					"aTargets": [ 5 ],
					"mData": 5,
					"visible" : false,
					"searchable" : false
				});
				
				indexesDataTable = $('#db-objects-list-results').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no indexes on this table"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
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
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: $(OBJECT_NAME_CONTAINER).text(), orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: $(OBJECT_NAME_CONTAINER).text() }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-list-results td').css("white-space","nowrap");
		             }
				});


				//$('#db-objects-list-results td').css("white-space","nowrap");
				$('#db-objects-list-results tbody').on( 'click', 'td', function (e, a) {
					if(indexesDataTable.cell(this)){
						var cell = indexesDataTable.cell(this).index();
						if(cell){
							if(cell.column == 0){
								var data = indexesDataTable.row(cell.row).data();
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
		displayStatistics: function(result){
			$(STATISTICS_SPINNER).hide();
			var keys = result.columnNames;
			$(STATISTICS_ERROR_CONTAINER).hide();
			pageStatus.statisticsFetched = true;
			
			if(keys != null && keys.length > 0) {
				$(STATISTICS_CONTAINER).show();
				var sb = '<table class="table table-striped table-bordered table-hover dbmgr-table" id="db-object-statistics-list"></table>';
				$(STATISTICS_CONTAINER).html( sb );

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

				if(statisticsTable != null) {
					try {
						statisticsTable.clear().draw();
					}catch(Error){

					}
				}
				var aoColumnDefs = [];
				aoColumnDefs.push({
						"aTargets": [ 9 ],
						"mData": 9,
						"mRender": function ( data, type, full ) {
							if (type === 'display') {
								return common.toServerLocalDateFromUtcMilliSeconds(data);  
							}
							else return data;
		            	 }
					});	
				
				statisticsTable = $('#db-object-statistics-list').DataTable({
					"oLanguage": {
						"sEmptyTable": "There are no statistics"
					},
					dom: '<"top"l<"clear">Bf>t<"bottom"rip>',
					processing: true,
					paging: bPaging,
					autoWidth: true,
					"iDisplayLength" : 25, 
					"sPaginationType": "full_numbers",
					"aaData": aaData, 
					"aoColumns" : aoColumns,
					aoColumnDefs: aoColumnDefs,
					"order": [[ 0, "asc" ]],
	                 buttons: [
	                           { extend : 'copy', exportOptions: { columns: ':visible' } },
	                           { extend : 'csv', exportOptions: { columns: ':visible' } },
	                           { extend : 'excel', exportOptions: { columns: ':visible' } },
	                           { extend : 'pdfHtml5', exportOptions: { columns: ':visible' }, title: "Statistics for "+routeArgs.type + " " + routeArgs.name, orientation: 'landscape' },
	                           { extend : 'print', exportOptions: { columns: ':visible' }, title: "Statistics for "+routeArgs.type + " " + routeArgs.name }
	                           ],					             
		             fnDrawCallback: function(){
		            	// $('#db-object-statistics-list td').css("white-space","nowrap");
		             }
				});
			}
		},		
		fetchAttributesError: function (jqXHR) {
			$(ATTRIBUTES_SPINNER).hide();
			$(ATTRIBUTES_ERROR_CONTAINER).show();
			$(ATTRIBUTES_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(ATTRIBUTES_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(ATTRIBUTES_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		},
		fetchDDLError: function (jqXHR) {
			$(DDL_SPINNER).hide();
			$(DDL_ERROR_CONTAINER).show();
			$(DDL_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(DDL_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(DDL_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		}, 
		fetchColumnsError: function (jqXHR) {
			$(COLUMNS_SPINNER).hide();
			$(COLUMNS_ERROR_CONTAINER).show();
			$(COLUMNS_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(COLUMNS_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(COLUMNS_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		},  
		fetchRegionsError: function (jqXHR) {
			$(REGIONS_SPINNER).hide();
			$(REGIONS_ERROR_CONTAINER).show();
			$(REGIONS_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(REGIONS_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(REGIONS_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		},  
		fetchPrivilegesError: function (jqXHR) {
			$(PRIVILEGES_SPINNER).hide();
			$(PRIVILEGES_ERROR_CONTAINER).show();
			$(PRIVILEGES_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(PRIVILEGES_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(PRIVILEGES_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		}, 
		fetchUsagesError: function (jqXHR) {
			$(USAGES_SPINNER).hide();
			$(USAGES_ERROR_CONTAINER).show();
			$(USAGES_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(USAGES_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(USAGES_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		},  
		fetchIndexesError: function (jqXHR) {
			$(INDEXES_SPINNER).hide();
			$(INDEXES_ERROR_CONTAINER).show();
			$(INDEXES_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(INDEXES_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(INDEXES_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		}, 
		fetchStatisticsError: function (jqXHR) {
			$(STATISTICS_SPINNER).hide();
			$(STATISTICS_ERROR_CONTAINER).show();
			$(STATISTICS_CONTAINER).hide();
			if (jqXHR.responseText) {
				$(STATISTICS_ERROR_CONTAINER).text(jqXHR.responseText);
			}else{
				if(jqXHR.status != null && jqXHR.status == 0) {
					$(STATISTICS_ERROR_CONTAINER).text("Error : Unable to communicate with the server.");
				}
			}
		},  
	});


	return SchemaObjectDetailView;
});
