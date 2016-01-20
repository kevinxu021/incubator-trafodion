// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

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
        'views/alerts/AlertsSummaryView',
        'views/alerts/AlertDetailView',
        'views/help/AboutView',
        'model/Session',
        'model/Localizer',
        'metismenu'
        ], function($, _, Backbone, NavbarView, DashboardView, WorkbenchView, DCSServerView, LoginView, 
        		SchemasView, SchemaDetailView, SchemaObjectsView, SchemaObjectDetailView,
        		ActiveWorkloadsView, ActiveQueryDetailView, HistoricalWorkloadsView, HistoricalWorkloadDetailView, QueryPlanView, 
        		LogsView, CreateLibraryView, AlertsSummaryView, AlertDetailView, AboutView, Session, Localizer) {
	'use strict';

	var currentSelection = null;
	var currentView = null;
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
	var alertsSummaryView = null;
	var alertDetailView = null;
	var aboutView = null;
	
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
			'tools/createlibrary':'createLibrary',
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
		
		topOffset = 50;
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
		currentView = null;
		dashboardView = null;
		workbenchView = null;
		dcsServerView = null;
		schemasView = null;
		schemaDetailView = null;
		schemaObjectsView = null;
		schemaObjectDetailView = null;
		historicalWorkloadsView = null;
		historicalWorkloadDetailView = null;
		activeWorkloadsView = null;
		activeQueryDetailView = null;
		queryPlanView = null
		logsView = null;		
		alertsSummaryView = null;
		alertDetailView = null;
		aboutView = null;
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
			if(loginView == null)
				loginView = new LoginView(app_router);

			loginView.init();
			loginView.render();
		});

		app_router.on('route:showDashboard', function(){

			// Call render on the module we loaded in via the dependency array
			if(dashboardView == null)
				dashboardView = new DashboardView();

			switchView(dashboardView);
		});

		app_router.on('route:showWorkbench', function () {
			if(workbenchView == null)
				workbenchView = new WorkbenchView();
			switchView(workbenchView);
		});

		app_router.on('route:showDcsServers', function () {
			if(dcsServerView == null)
				dcsServerView = new DCSServerView();
			switchView(dcsServerView);
		});

		app_router.on('route:showSchemas', function () {
			if(schemasView == null)
				schemasView = new SchemasView();	
			
			switchView(schemasView);
		});
		
		app_router.on('route:showSchemaDetail', function () {
			
			var args = deparam();
			
			if(schemaDetailView == null)
				schemaDetailView = new SchemaDetailView();	
			
			switchView(schemaDetailView, args);
		});

		app_router.on('route:showSchemaObjects', function (args, params) {
			var args = deparam();
			if(schemaObjectsView == null)
				schemaObjectsView = new SchemaObjectsView();	
			
			switchView(schemaObjectsView, args);
		});

		app_router.on('route:showSchemaObjectDetail', function (args, params) {
			var args = deparam();

			if(schemaObjectDetailView == null)
				schemaObjectDetailView = new SchemaObjectDetailView();	
			
			switchView(schemaObjectDetailView, args);
		});

		app_router.on('route:showHistoricalWorkloads', function (args) {
			if(historicalWorkloadsView == null)
				historicalWorkloadsView = new HistoricalWorkloadsView();
			switchView(historicalWorkloadsView, args);
		});
		
		app_router.on('route:showHistoricalWorkloadDetail', function (args) {
			if(historicalWorkloadDetailView == null)
				historicalWorkloadDetailView = new HistoricalWorkloadDetailView();
			switchView(historicalWorkloadDetailView, args);
		});
		
		app_router.on('route:showActiveWorkloads', function (args) {
			if(activeWorkloadsView == null)
				activeWorkloadsView = new ActiveWorkloadsView();
			switchView(activeWorkloadsView, args);
		});		
		
		app_router.on('route:showActiveQueryDetail', function (args) {
			if(activeQueryDetailView == null)
				activeQueryDetailView = new ActiveQueryDetailView();
			switchView(activeQueryDetailView, args);
		});	
		
		app_router.on('route:showQueryPlan', function (args) {
			if(queryPlanView == null)
				queryPlanView = new QueryPlanView();
			switchView(queryPlanView, args);
		});	
		
		app_router.on('route:showLogs', function (args) {
			if(logsView == null)
				logsView = new LogsView();
			switchView(logsView, args);
		});		
		
		app_router.on('route:showAlertsSummary', function(args){
			if(alertsSummaryView == null)
				alertsSummaryView = new AlertsSummaryView();
			switchView(alertsSummaryView, args);
		});
		
		app_router.on('route:showAlertDetail', function(args){
			if(alertDetailView == null)
				alertDetailView = new AlertDetailView();
			switchView(alertDetailView, args);
		});
		
		app_router.on('route:createLibrary', function (args) {
			if(createLibraryView == null)
				createLibraryView = new CreateLibraryView();
			switchView(createLibraryView, args);
		});
		
		app_router.on('route:showAbout', function (args) {
			if(aboutView == null)
				aboutView = new AboutView();
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
			if(dashboardView == null)
				dashboardView = new DashboardView();

			switchView(dashboardView);
			
		});


		Backbone.history.start();
	};
	return { 
		initialize: initialize
	};
});
