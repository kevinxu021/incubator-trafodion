define([
        'jquery',
        'underscore',
        'backbone',
        'views/navbar/NavbarView',
        'views/dashboard/DashboardView',
        'views/workbench/WorkbenchView',
        'views/dcs/DCSServerView',
        'views/login/LoginView',
        'views/database/DatabaseView',
        'views/workloads/ActiveWorkloadsView',
        'views/workloads/ActiveQueryDetailView',
        'views/workloads/HistoricalWorkloadsView',
        'views/workloads/HistoricalWorkloadDetailView',
        'views/workloads/QueryPlanView',
        'views/logs/LogsView',
        'model/Session',
        'model/Localizer',
        'metismenu'
        ], function($, _, Backbone, NavbarView, DashboardView, WorkbenchView, DCSServerView, LoginView, DatabaseView, 
        		ActiveWorkloadsView, ActiveQueryDetailView, HistoricalWorkloadsView, HistoricalWorkloadDetailView, QueryPlanView, 
        		LogsView, Session, Localizer) {
	'use strict';

	var currentSelection = null;
	var currentView = null;
	var dashboardView = null;
	var workbenchView = null;
	var dcsServerView = null;
	var loginView = null;
	var databaseView = null;
	var historicalWorkloadsView = null;
	var historicalWorkloadDetailView = null;
	var activeWorkloadsView = null;
	var activeQueryDetailView = null;
	var queryPlanView = null;
	var logsView = null;
	
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
			'database(/*args)' : 'showDatabase',
			'login': 'showLogin',
			'logout': 'doLogout',
			'workloads/active': 'showActiveWorkloads',
			'workloads/history': 'showHistoricalWorkloads',
			'workloads/history/querydetail(/*args)':'showHistoricalWorkloadDetail',
			'workloads/active/querydetail(/*args)':'showActiveQueryDetail',
			'workloads/history/queryplan(/*args)':'showQueryPlan',
			'logs': 'showLogs',
			'tools/(*args)': 'showTools',
			// Default
			'*actions': 'defaultAction'
		}
	});

	var switchView = function(view, args) {
		
		topOffset = 50;
		$('#side-menu').metisMenu();
		
		if (currentView && currentView != view) {
			// Detach the old view
			currentView.remove();
		}
		// Render view after it is in the DOM (styles are applied)
		view.render(args);
		currentView = view;
	};

	var logout = function(){
		currentView = null;
		$('#sessionUserName').html('<i class="fa fa-user fa-fw"></i>');
		if(loginView != null){
			loginView.doLogout();
		}
		window.location.hash = '/login';
		currentView = null;
		dashboardView = null;
		workbenchView = null;
		dcsServerView = null;
		databaseView = null;	
		historicalWorkloadsView = null;
		historicalWorkloadDetailView = null;
		activeWorkloadsView = nul;
		activeQueryDetailView = null;
		queryPlanView = null
		logsView = null;		
	}

	var initialize = function(){


		$.ajaxSetup({
			statusCode : {
				401 : logout,
				403 : logout
			}
		});

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

		app_router.on('route:showDatabase', function (args) {
			if(databaseView == null)
				databaseView = new DatabaseView();
			switchView(databaseView, args);
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

		app_router.on('route:showTools', function (args) {
		});	
		
		app_router.on('route:doLogout', function () {
			logout();
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
