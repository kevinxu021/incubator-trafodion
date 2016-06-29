// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
define([
  'jquery', 
  'underscore', 
  'backbone',
  'router',// Request router.js
  'handlers/ServerHandler',
  'jquerylocalize'
], function($, _, Backbone, Router,serverHandler){
  var initialize=function(){
	  $("[data-localize]").localize("/lang/dbmgr", { language: navigator.language });
	  serverHandler.setEsgynLocale(navigator.language);
  }
  return { 
	  initialize: initialize
  };
});
