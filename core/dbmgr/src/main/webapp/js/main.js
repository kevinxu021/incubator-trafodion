//@@@ START COPYRIGHT @@@

//(C) Copyright 2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

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
			exports: 'bootstrap'
		},
		jqueryui: {
			deps: ['jquery'],
			exports: 'jqueryui'
		},
		backbone: {
			// Depends on underscore and jQuery
			deps: ["underscore", "jquery"],
			exports: 'Backbone'
		},
		underscore: {
			exports: '_'
		},
		metismenu: {
			deps: ['jquery']
		},
		sbadmin2: {
			deps:['jquery','metismenu']
		},
		"datatables.net-bs": {
			deps: ['datatables.net']
		},
		buttonsbootstrap: {
			deps:['datatables.net']
		},
		buttonsflash: {
			deps:['datatables.net']
		},
		buttonsprint: {
			deps:['datatables.net']
		},
		buttonshtml: {
			deps:['datatables.net']
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
		},
		flot : {
			deps : ['jquery'],
			exports: 'flot'
		},
		flottime : {
			deps : ['flot'],
			exports: 'flottime'
		},
		flotcanvas : {
			deps : ['flot'],
			exports: 'flotcanvas'
		},
		flotcrosshair : {
			deps : ['flot'],
			exports: 'flotcrosshair'
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
		jqueryui: '../bower_components/jquery-ui/jquery-ui.min',
		underscore: '../bower_components/underscore/underscore-min',
		backbone: '../bower_components/backbone/backbone-min',
		templates: '../templates',
		metismenu: '../bower_components/metisMenu/dist/metisMenu',
		jit: '../bower_components/jit/Jit/jit',
		"datatables.net": '../bower_components/datatables.net/js/jquery.dataTables.min',
		"datatables.net-bs": '../bower_components/datatables.net-bs/js/dataTables.bootstrap.min',
		"datatables.net-select": '../bower_components/datatables.net-select/js/dataTables.select.min',
		"datatables.net-buttons": '../bower_components/datatables.net-buttons/js/dataTables.buttons.min',
		buttonsbootstrap: '../bower_components/datatables.net-buttons-bs/js/buttons.bootstrap.min',
		buttonsflash: '../bower_components/datatables.net-buttons/js/buttons.flash.min',
		buttonsprint: '../bower_components/datatables.net-buttons/js/buttons.print.min',
		buttonshtml: '../bower_components/datatables.net-buttons/js/buttons.html5.min',
		responsivetable: '../bower_components/datatables.net-responsive/js/dataTables.responsive.min',
		jstree: '../bower_components/jstree/dist/jstree',
		moment: '../bower_components/moment/min/moment.min',
		momenttimezone: '../bower_components/moment-timezone/builds/moment-timezone-with-data.min',
		datetimepicker: '../bower_components/eonasdan-bootstrap-datetimepicker/build/js/bootstrap-datetimepicker.min',
		pdfmake: '../bower_components/pdfmake/build/vfs_fonts',
		pdfMakeLib: '../bower_components/pdfmake/build/pdfmake.min',
		flot: '../bower_components/flot/jquery.flot',
		flottime: '../bower_components/flot/jquery.flot.time',
		flotcanvas: '../bower_components/flot/jquery.flot.canvas',
		flotcrosshair: '../bower_components/flot/jquery.flot.crosshair',
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
