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
        'datatablesBootStrap'
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

	var SchemaDetailView = BaseView.extend({
		template:  _.template(DatabaseT),

		doInit: function (args){
			_this = this;
			routeArgs = args;
			schemaName = routeArgs.name;
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
				pageStatus = {};
	        	if(ddlTextEditor){
	        		ddlTextEditor.setValue("");
	        		setTimeout(function() {
	        			ddlTextEditor.refresh();
	        		},1);
	        	}
			}	
			var TAB_LINK = $(SCHEMA_DETAILS_CONTAINER+' .tab-pane.active');
			if(TAB_LINK){
				var selectedTab = '#'+TAB_LINK.attr('id');
				if(selectedTab == ATTRIBUTES_SELECTOR || selectedTab == DDL_SELECTOR || selectedTab == PRIVILEGES_SELECTOR){
					//no-op
				}else{
					$(SCHEMA_DETAILS_CONTAINER +' a:first').tab('show');
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
			$(LOADING_SELECTOR).hide();$('#db-object-feature-selector .active').attr('id')
		},
		selectFeature: function(e){
			$(SCHEMA_DETAILS_CONTAINER).show();
			var selectedTab = ATTRIBUTES_SELECTOR;
			var TAB_LINK = $(SCHEMA_DETAILS_CONTAINER+' .tab-pane.active');
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
			case DDL_SELECTOR:
				$(DDL_CONTAINER).show();
				var ddlText = ddlTextEditor.getValue();
				if(ddlText == null || ddlText.length == 0){
					_this.fetchDDLText();
				}

				break;
			case PRIVILEGES_SELECTOR:
				$(PRIVILEGES_CONTAINER).show();
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
			_this.processRequest();
			$(ERROR_CONTAINER).hide();
		},
		fetchDDLText: function(){
			if(!pageStatus.ddl || pageStatus.ddl == false){
				_this.showLoading();
				dbHandler.fetchDDL('schema', routeArgs.name, null);
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
			$(PRIVILEGES_BTN).hide();
			$(TABLES_BTN).show();
			$(VIEWS_BTN).show();
			$(INDEXES_BTN).show();
			$(LIBRARIES_BTN).show();
			$(PROCEDURES_BTN).show();					
			_this.selectFeature();
		},
		fetchAttributes: function () {
			var attrs = sessionStorage.getItem(routeArgs.name);	
			if(attrs == null){
				//TO DO. Fetch from database.
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
			pageStatus.ddl = true;
			_this.hideLoading();
			ddlTextEditor.setValue(data);
			ddlTextEditor.refresh();
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
