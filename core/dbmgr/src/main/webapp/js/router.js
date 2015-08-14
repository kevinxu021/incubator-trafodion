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
        'views/workloads/WorkloadsView',
        'views/logs/LogsView',
        'model/Session',
        'metismenu'
        ], function($, _, Backbone, NavbarView, DashboardView, WorkbenchView, DCSServerView, LoginView, DatabaseView, 
        		WorkloadsView, LogsView, Session) {
	'use strict';

	var currentSelection = null;
	var currentView = null;
	var dashboardView = null;
	var workbenchView = null;
	var dcsServerView = null;
	var loginView = null;
	var databaseView = null;
	var workloadsView = null;
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
			'workloads': 'showWorkloads',
			'logs': 'showLogs',
			'tools/(*args)': 'showTools',
			// Default
			'*actions': 'defaultAction'
		}
	});

	var switchView = function(view, args) {
		
		topOffset = 50;
		$('#side-menu').metisMenu();
		
       /* width = (this.innerWidth > 0) ? this.innerWidth : this.screen.width;
        if (width < 768) {
            $('div.navbar-collapse').addClass('collapse');
            topOffset = 100; // 2-row-menu
        } else {
            $('div.navbar-collapse').removeClass('collapse');
        }

        height = ((this.innerHeight > 0) ? this.innerHeight : this.screen.height) - 1;
        height = height - topOffset;
        if (height < 1) height = 1;
        if (height > topOffset) {
            $("#page-wrapper").css("min-height", (height) + "px");
        }*/
		
		//$('.sidebar-nav ul li a').removeClass("active");
		//$('#side-menu li a').removeClass("active");
		if (currentView && currentView != view) {
			// Detach the old view
			currentView.remove();
		}

		// Move the view element into the DOM (replacing the old content)
		//this.el.html(view.el);

		// Render view after it is in the DOM (styles are applied)
		view.render(args);
		currentView = view;
        
		/*var url = window.location;
        var element = $('ul.nav a').filter(function() {
            //if(this.href == url || url.href.indexOf(this.href) == 0){
            if(this.href == url || url.href == this.href){
            	currentSelection = this;
            	return true;
            }else{
            	return false;
            }
        }).addClass('active').parent().parent().addClass('in').parent();
        
        if (element.is('li')) {
            element.addClass('active');
        }*/
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
		workloadsView = null;
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
		
		app_router.on('route:showWorkloads', function (args) {
			if(workloadsView == null)
				workloadsView = new WorkloadsView();
			switchView(workloadsView, args);
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
