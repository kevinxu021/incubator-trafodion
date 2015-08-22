require.config({
	shim: {
		bootstrap: {
			deps: ['jquery'],
			exports: 'jquery'
		},
		backbone: {
			// Depends on underscore and jQuery
			deps: ["underscore", "jquery"],
			exports: 'Backbone'
		},
		underscore: {
			exports: '_'
		},
		morris: {
			deps: ['jquery', 'raphael'],
			exports: 'Morris'
		},
		raphael: {
			deps: ['jquery'],
			exports: 'Raphael'
		},
		metismenu: {
			deps: ['jquery']
		},
		sbadmin2: {
			deps:['jquery','metismenu']
		},
		dataTablesBootStrap: {
			deps: ['dataTables']
		},
		morrisdata: {
			deps: ['morris']
		}
	},
	paths: {
		sbadmin2:'../bower_components/startbootstrap-sb-admin-2/dist/js/sb-admin-2',
		text: '../bower_components/text/text',
		bootstrap:'../bower_components/bootstrap/dist/js/bootstrap.min',
		jquery: '../bower_components/jquery/dist/jquery.min',
		jquerycookie: '../bower_components/jquery.cookie/jquery.cookie',
		jquerylocalize: '../bower_components/jquery-localize-i18n/dist/jquery.localize.min',
		jqueryvalidate: '../bower_components/jquery-validation/dist/jquery.validate.min',
		jqueryui: '../bower_components/jquery-ui/jquery-ui',
		underscore: '../bower_components/underscore/underscore-min',
		backbone: '../bower_components/backbone/backbone-min',
		morris: '../bower_components/morrisjs/morris.min',
		raphael: '../bower_components/raphael/raphael-min',
		templates: '../templates',
		metismenu: '../bower_components/metisMenu/dist/metisMenu',
		jit: '../bower_components/jit/Jit/jit',
		datatables: '../bower_components/datatables/media/js/jquery.dataTables.min',
		datatablesBootStrap: '../bower_components/datatables-plugins/integration/bootstrap/3/dataTables.bootstrap',
		datatablesUI: '../bower_components/datatables-plugins/integration/jqueryui/dataTables.jqueryui',
		tabletools: '../bower_components/datatables-tabletools/js/dataTables.tableTools',
		jstree: '../bower_components/jstree/dist/jstree',
		moment: '../bower_components/moment/min/moment.min',
		datetimepicker: '../bower_components/eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min'
	}

});

require([

         // Load our app module and pass it to our definition function
         'app'

         ], function(App){
	// The "app" dependency is passed in as "App"
	// Again, the other dependencies passed in are not "AMD" therefore don't pass a parameter to this function
	App.initialize();

});
