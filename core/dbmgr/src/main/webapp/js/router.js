//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone',
        'views/navbar/NavbarView',
        'views/dashboard/DashboardView',
        'views/workbench/WorkbenchView',
        'views/dcs/DCSServerView',
        'views/login/LoginView',
        'views/database/SchemasView',
        'views/database/SchemaDetailView',
        'views/database/SchemaObjectsView',
        'views/database/SchemaObjectDetailView',
        'views/workloads/ActiveWorkloadsView',
        'views/workloads/ActiveQueryDetailView',
        'views/workloads/HistoricalWorkloadsView',
        'views/workloads/HistoricalWorkloadDetailView',
        'views/workloads/QueryPlanView',
        'views/logs/LogsView',
        'views/tools/CreateLibraryView',
        'views/tools/AlterLibraryView',
        'views/alerts/AlertsSummaryView',
        'views/alerts/AlertDetailView',
        'views/help/AboutView',
        'model/Session',
        'model/Localizer',
        'views/workloads/WorkloadProfileConfigurationView',
        'views/workloads/WorkloadSLAConfigurationView',
        'views/workloads/WorkloadMappingConfigurationView',
        'views/tools/SQLConverterView',
        'views/tools/ScriptExecutorView',
        'metismenu'
        ], function($, _, Backbone, NavbarView, DashboardView, WorkbenchView, DCSServerView, LoginView, 
        		SchemasView, SchemaDetailView, SchemaObjectsView, SchemaObjectDetailView,
        		ActiveWorkloadsView, ActiveQueryDetailView, HistoricalWorkloadsView, HistoricalWorkloadDetailView, QueryPlanView, 
        		LogsView, CreateLibraryView, AlterLibraryView, AlertsSummaryView, AlertDetailView, AboutView, Session, Localizer,
        		WorkloadProfileConfigurationView, WorkloadSLAConfigurationView, WorkloadMappingConfigurationView, SQLConverterView,
        		ScriptExecutorView) {
	'use strict';

	var currentSelection = null;
	var currentView = null;
	var viewCollection = [];

	var dashboardView = null;
	var workbenchView = null;
	var dcsServerView = null;
	var loginView = null;
	var schemasView = null;
	var schemaDetailView = null;
	var schemaObjectsView = null;
	var schemaObjectDetailView = null;
	var historicalWorkloadsView = null;
	var historicalWorkloadDetailView = null;
	var activeWorkloadsView = null;
	var activeQueryDetailView = null;
	var queryPlanView = null;
	var logsView = null;
	var createLibraryView = null;
	var alterLibraryView = null;
	var alertsSummaryView = null;
	var alertDetailView = null;
	var aboutView = null;
	var sqlConverterView = null;
	var workloadProfileConfigurationView = null;
	var workloadSLAConfigurationView = null;
	var workloadMappingConfigurationView = null;
	var scriptExecutorView = null;

	var AppRouter = Backbone.Router.extend({
		execute: function(callback, args, name) {
			if (Session.getUser() == null) {
				window.location.hash = "/login";
				if(loginView == null)
					loginView = new LoginView();

				loginView.render();
				return false;
			}
			$('#navbar').show();
			$('#sessionUserName').html('<i class="fa fa-user fa-fw"></i>'+Session.getUser());
			if (callback) callback.apply(this, args);
		},
		routes: {
			// Define some URL routes
			'index': 'showDashboard',
			'workbench': 'showWorkbench',
			'dcsservers': 'showDcsServers',
			//'database(/:objecttype)(/:name)(/:feature)' : 'showDatabase',
			//'database(/*args)(?*:params)' : 'showDatabase',
			'database' : 'showSchemas',
			'database/schema(?*:params)': 'showSchemaDetail',
			'database/objects(?*:params)': 'showSchemaObjects',
			'database/objdetail(?*:params)': 'showSchemaObjectDetail',
			'login': 'showLogin',
			'logout': 'doLogout',
			'stimeout': 'doSessionTimeout',
			'workloads/active': 'showActiveWorkloads',
			'workloads/history': 'showHistoricalWorkloads',
			'workloads/history/querydetail(/*args)':'showHistoricalWorkloadDetail',
			'workloads/active/querydetail(/*args)':'showActiveQueryDetail',
			'workloads/queryplan(/*args)':'showQueryPlan',
			'workloads/configuration/profiles':'showWorkloadProfiles',
			'workloads/configuration/slas':'showWorkloadSLAs',
			'workloads/configuration/mappings':'showWorkloadMappings',
			'tools/createlibrary(?*:params)':'createLibrary',
			'tools/alterlibrary(?*:params)':'alterLibrary',
			'tools/sqlconverter':'showSQLConverter',
			'tools/executescript':'showScriptExecutor',
			'alerts': 'showAlertsSummary',
			'alert/detail(/*args)': 'showAlertDetail',
			'help/about': 'showAbout',
			'help/userguide': 'showUserGuide',
			'logs': 'showLogs',
			'tools/(*args)': 'showTools',
			// Default
			'*actions': 'defaultAction'
		}
	});

	var deparam = function(){
		var urlHash = window.location.hash;
		var paramIndex = urlHash.indexOf('?');

		var result = {};
		if( paramIndex < 0){
			return result;
		}
		var paramString = urlHash.substring(paramIndex + 1);
		$.each(paramString.split('&'), function(index, value){
			if(value){
				var param = value.split('=');
				result[param[0]] = param[1];
			}
		});
		return result;
	};

	var switchView = function(view, args) {

		$('#side-menu').metisMenu();

		if (currentView && (currentView != view || currentView == schemasView)) {
			// Detach the old view
			currentView.remove();
		}
		// Render view after it is in the DOM (styles are applied)
		view.render(args);
		currentView = view;
	};

	var logout = function(){
		if(currentView != null){
			currentView.pause();
		}
		currentView = null;
		Session.eraseAll();
		$('#sessionUserName').html('<i class="fa fa-user fa-fw"></i>');
		if(loginView != null){
			loginView.doLogout();
		}
		window.location.hash = '/login';

		$.each(viewCollection, function(i, v){
			if(v.doCleanup){
				v.doCleanup();
			}
			v = null;
		});
		viewCollection = [];

		dashboardView = null;
		workbenchView = null;
		dcsServerView = null;
		loginView = null;
		schemasView = null;
		schemaDetailView = null;
		schemaObjectsView = null;
		schemaObjectDetailView = null;
		historicalWorkloadsView = null;
		historicalWorkloadDetailView = null;
		activeWorkloadsView = null;
		activeQueryDetailView = null;
		queryPlanView = null;
		logsView = null;
		createLibraryView = null;
		alterLibraryView = null;
		alertsSummaryView = null;
		alertDetailView = null;
		aboutView = null;
		workloadProfileConfigurationView = null;
		workloadSLAConfigurationView = null;
		workloadMappingConfigurationView = null;
		sqlConverterView = null;
		scriptExecutorView = null;
		currentView = null;
	};

	var sessionTimeOut = function(){
		logout();
		if(loginView != null) {
			loginView.sessionTimedOut = true;
		}
	};

	var initialize = function(){

		var navV = new NavbarView();
		navV.render();

		var app_router = new AppRouter;

		app_router.on('route:showLogin', function(){

			// Call render on the module we loaded in via the dependency array
			if(loginView == null){
				loginView = new LoginView(app_router);
				viewCollection.push(loginView);
			}

			loginView.init();
			loginView.render();
		});

		app_router.on('route:showDashboard', function(){

			// Call render on the module we loaded in via the dependency array
			if(dashboardView == null){
				dashboardView = new DashboardView();
				viewCollection.push(dashboardView);
			}

			switchView(dashboardView);
		});

		app_router.on('route:showWorkbench', function () {
			if(workbenchView == null){
				workbenchView = new WorkbenchView();
				viewCollection.push(workbenchView);
			}
			switchView(workbenchView);
		});

		app_router.on('route:showDcsServers', function () {
			if(dcsServerView == null){
				dcsServerView = new DCSServerView();
				viewCollection.push(dcsServerView);
			}
			switchView(dcsServerView);
		});

		app_router.on('route:showSchemas', function () {
			if(schemasView == null){
				schemasView = new SchemasView();	
				viewCollection.push(schemasView);
			}

			switchView(schemasView);
		});

		app_router.on('route:showSchemaDetail', function () {

			var args = deparam();

			if(schemaDetailView == null){
				schemaDetailView = new SchemaDetailView();	
				viewCollection.push(schemaDetailView);
			}

			switchView(schemaDetailView, args);
		});

		app_router.on('route:showSchemaObjects', function (args, params) {
			var args = deparam();
			if(schemaObjectsView == null){
				schemaObjectsView = new SchemaObjectsView();	
				viewCollection.push(schemaObjectsView);
			}

			switchView(schemaObjectsView, args);
		});

		app_router.on('route:showSchemaObjectDetail', function (args, params) {
			var args = deparam();

			if(schemaObjectDetailView == null){
				schemaObjectDetailView = new SchemaObjectDetailView();	
				viewCollection.push(schemaObjectDetailView);
			}

			switchView(schemaObjectDetailView, args);
		});

		app_router.on('route:showHistoricalWorkloads', function (args) {
			if(historicalWorkloadsView == null){
				historicalWorkloadsView = new HistoricalWorkloadsView();
				viewCollection.push(historicalWorkloadsView);
			}
			switchView(historicalWorkloadsView, args);
		});

		app_router.on('route:showHistoricalWorkloadDetail', function (args) {
			if(historicalWorkloadDetailView == null){
				historicalWorkloadDetailView = new HistoricalWorkloadDetailView();
				viewCollection.push(historicalWorkloadDetailView);
			}
			switchView(historicalWorkloadDetailView, args);
		});

		app_router.on('route:showActiveWorkloads', function (args) {
			if(activeWorkloadsView == null){
				activeWorkloadsView = new ActiveWorkloadsView();
				viewCollection.push(activeWorkloadsView);
			}
			switchView(activeWorkloadsView, args);
		});		

		app_router.on('route:showActiveQueryDetail', function (args) {
			if(activeQueryDetailView == null){
				activeQueryDetailView = new ActiveQueryDetailView();
				viewCollection.push(activeQueryDetailView);
			}
			switchView(activeQueryDetailView, args);
		});	

		app_router.on('route:showWorkloadProfiles', function (args) {
			if(workloadProfileConfigurationView == null){
				workloadProfileConfigurationView = new WorkloadProfileConfigurationView();
				viewCollection.push(workloadProfileConfigurationView);
			}
			switchView(workloadProfileConfigurationView, args);
		});	

		app_router.on('route:showWorkloadSLAs', function (args) {
			if(workloadSLAConfigurationView == null){
				workloadSLAConfigurationView = new WorkloadSLAConfigurationView();
				viewCollection.push(workloadSLAConfigurationView);
			}
			switchView(workloadSLAConfigurationView, args);
		});	

		app_router.on('route:showWorkloadMappings', function (args) {
			if(workloadMappingConfigurationView == null){
				workloadMappingConfigurationView = new WorkloadMappingConfigurationView();
				viewCollection.push(workloadMappingConfigurationView);
			}
			switchView(workloadMappingConfigurationView, args);
		});	

		app_router.on('route:showQueryPlan', function (args) {
			if(queryPlanView == null){
				queryPlanView = new QueryPlanView();
				viewCollection.push(queryPlanView);
			}
			switchView(queryPlanView, args);
		});	

		app_router.on('route:showLogs', function (args) {
			if(logsView == null){
				logsView = new LogsView();
				viewCollection.push(logsView);
			}
			switchView(logsView, args);
		});		

		app_router.on('route:showAlertsSummary', function(args){
			if(alertsSummaryView == null){
				alertsSummaryView = new AlertsSummaryView();
				viewCollection.push(alertsSummaryView);
			}
			switchView(alertsSummaryView, args);
		});

		app_router.on('route:showAlertDetail', function(args){
			if(alertDetailView == null){
				alertDetailView = new AlertDetailView();
				viewCollection.push(alertDetailView);
			}
			switchView(alertDetailView, args);
		});

		app_router.on('route:createLibrary', function (args, params) {
			var args = deparam();

			if(createLibraryView == null){
				createLibraryView = new CreateLibraryView();
				viewCollection.push(createLibraryView);
			}
			switchView(createLibraryView, args);
		});

		app_router.on('route:alterLibrary', function (args, params) {
			var args = deparam();

			if(alterLibraryView == null){
				alterLibraryView = new AlterLibraryView();
				viewCollection.push(alterLibraryView);
			}
			switchView(alterLibraryView, args);
		});

		app_router.on('route:showSQLConverter', function (args, params) {
			var args = deparam();
			if(sqlConverterView == null){
				sqlConverterView = new SQLConverterView();
				viewCollection.push(sqlConverterView);
			}
			switchView(sqlConverterView, args);
		});

		app_router.on('route:showScriptExecutor', function (args, params) {
			var args = deparam();
			if(scriptExecutorView == null){
				scriptExecutorView = new ScriptExecutorView();
				viewCollection.push(scriptExecutorView);
			}
			switchView(scriptExecutorView, args);
		});

		app_router.on('route:showAbout', function (args) {
			if(aboutView == null){
				aboutView = new AboutView();
				viewCollection.push(aboutView);
			}
			switchView(aboutView, args);
		});	

		app_router.on('route:showTools', function (args) {
		});	

		app_router.on('route:doLogout', function () {
			logout();
		});

		app_router.on('route:showUserGuide', function(){
			var uri = window.location.protocol+'://'+window.location.host+'/docs/';
			window.open(uri);
		});

		app_router.on('route:doSessionTimeout', function(){
			logout();
			if(loginView != null) {
				loginView.sessionTimedOut = true;
			}
		});

		app_router.on('route:defaultAction', function (actions) {

			// We have no matching route, lets display the home page 
			if(dashboardView == null){
				dashboardView = new DashboardView();
				viewCollection.push(dashboardView);
			}

			switchView(dashboardView);

		});


		Backbone.history.start();
	};
	return { 
		initialize: initialize
	};
});
