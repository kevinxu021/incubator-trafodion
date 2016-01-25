// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

require.config({
	packages: [{
		name: "codemirror",
		location: "../bower_components/codemirror/lib",
		main: "codemirror"
	},
	{
		name: "codesqljs",
		location: "../bower_components/codemirror/mode/sql",
		main: "sql"
	},
	{
		name: "codeshowhint",
		location: "../bower_components/codemirror/addon/hint",
		main: "show-hint"
	},
	{
		name: "codesqlhint",
		location: "../bower_components/codemirror/addon/hint",
		main: "sql-hint"
	}
	],
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
			deps: ['datatables']
		},
		buttonsbootstrap: {
			deps:['datatables']
		},
		buttonsflash: {
			deps:['datatables']
		},
		buttonsprint: {
			deps:['datatables']
		},
		buttonshtml: {
			deps:['datatables']
		},		
		morrisdata: {
			deps: ['morris']
		},
		pdfMakeLib :
        {           
            exports: 'pdfmake'
        },
        pdfmake : 
        {
            deps: ['pdfMakeLib'],
            exports: 'pdfmake'
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
		tablebuttons: '../bower_components/datatables-buttons/js/dataTables.buttons',
		buttonsbootstrap: '../bower_components/datatables-buttons/js/buttons.bootstrap',
		buttonsflash: '../bower_components/datatables-buttons/js/buttons.flash',
		buttonsprint: '../bower_components/datatables-buttons/js/buttons.print',
		buttonshtml: '../bower_components/datatables-buttons/js/buttons.html5',
		responsivetable: '../bower_components/datatables-responsive/js/dataTables.responsive',
		jstree: '../bower_components/jstree/dist/jstree',
		moment: '../bower_components/moment/min/moment.min',
		momenttimezone: '../bower_components/moment-timezone/builds/moment-timezone-with-data.min',
		datetimepicker: '../bower_components/eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min',
		pdfmake: '../bower_components/pdfmake/build/vfs_fonts',
		pdfMakeLib: '../bower_components/pdfmake/build/pdfmake.min'
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
