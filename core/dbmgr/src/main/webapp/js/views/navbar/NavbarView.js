// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

define([
    'jquery',
    'underscore',
    'backbone',
    'bootstrap',
    'text!templates/navbar.html'
], function ($, _, Backbone, Bootstrap, NavbarT) {
    'use strict';

    var NavbarView = Backbone.View.extend({

        template: _.template(NavbarT),

        el: $('#navbar'),
        
        initialize: function () {
            //this.listenTo(this.model, 'change', this.render);
        },

        render: function () {
            this.$el.html(this.template);
        }
    });

    return NavbarView;
});
