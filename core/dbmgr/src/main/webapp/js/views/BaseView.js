define([
        'jquery',
        'underscore',
        'backbone',
        'jqueryui',
        ], function ($, _, Backbone) {
	'use strict';

	var BaseView = Backbone.View.extend({

		el: $('#wrapper'),

		initialized: false,

		pageWrapper: null,

		/*initialize: function () {

		},*/

		initialize: function(options) { 
			_.bindAll(this, 'beforeRender', 'render', 'afterRender'); 
			var _this = this; 
			this.render = _.wrap(this.render, function(render, args) { 
				_this.beforeRender(); 
				render(args); 
				_this.afterRender(); 
				return _this; 
			}); 
		}, 

		beforeRender: function() { 
			console.log('beforeRender'); 
		}, 

		render: function (args) {
			if(this.pageWrapper == null){
				this.$el.html(this.template);
				this.init(args);
			}else
			{
				//this.pause();
				this.$el.empty().append(this.pageWrapper);
				this.resume(args);
			}
			return this;        	
		},
		
		afterRender: function() { 
			console.log('afterRender'); 
		},

		init: function(){

		},
		resume: function(){

		},
		pause: function() {

		},

		remove: function(){
			var childElement = $(this.$el[0]).find('#page-wrapper');
			if(childElement && childElement.length > 0)
				this.pageWrapper = childElement.detach();
			this.pause();
		},
	});

	return BaseView;
});
