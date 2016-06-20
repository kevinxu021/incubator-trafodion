//@@@ START COPYRIGHT @@@

//(C) Copyright 2015-2016 Esgyn Corporation

//@@@ END COPYRIGHT @@@

define([
        'jquery',
        'underscore',
        'backbone',
        'common',
        'handlers/ServerHandler',
        'bootstrapNotify',
        'jqueryui'
        ], function ($, _, Backbone, common, serverHandler) {
	'use strict';
	var _this = null;
	var __this=null;
	var resizeTimer = null,
		sidebarTimer = null;

	var BaseView = Backbone.View.extend({

		el: $('#content-wrapper'),
		
		DETAIL_EL:$('#notifyMenu>ul.even'),

		initialized: false,

		TASK_MENU:'#notify',

		pageWrapper: null,

		pageIdentifier:null,

		currentURL:null,

		redirectFlag:null,
		/*initialize: function () {

		},*/
		refreshTimer: (function() {
			var _interval;
			var _intervalId;
			var _refreshTimer = {};

			_refreshTimer.set = function(newInterval) {
				if(newInterval){
					if(typeof newInterval === 'string'){
						var value = parseFloat(newInterval);
						if(!isNaN(value)) {
							_interval = value * 60 * 1000;
						}
					}else{
						_interval = newInterval * 60 * 1000;
					}

					if(_interval != 0){
						_refreshTimer.start();
					}else{
						_refreshTimer.stop();
					}
				}else{
					_refreshTimer.stop();
				}

			};

			_refreshTimer.get = function() {
				return _interval;
			};

			_refreshTimer.beep = function() {
				// console.log("========beep==========");
				if(_this.onTimerBeeped){
					_this.onTimerBeeped();
				}
			};

			_refreshTimer.start = function() {
				_refreshTimer.stop();
				if (_interval) {
					_intervalId = window.setInterval(_refreshTimer.beep, _interval);
				}
			};

			_refreshTimer.stop = function() {
				if (_intervalId) {
					window.clearInterval(_intervalId);
				}
			};

			return _refreshTimer;
		}()),

		initialize: function(options) { 
			_.bindAll(this, 'beforeRender', 'render', 'afterRender'); 
			_this = this; 

			this.render = _.wrap(this.render, function(render, args) { 
				_this.beforeRender(); 
				render(args); 
				_this.afterRender(); 
				return _this; 
			}); 
		}, 

		beforeRender: function() { 
		}, 

		render: function (args) {
			if(this.pageWrapper == null){
				this.$el.html(this.template);
				if(common.serverTimeZone == null){
					serverHandler.loadServerConfig();
				}
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
		},

		init: function(args){
			__this=this;
			common.on(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
			common.on(common.SIDEBAR_TOGGLE_EVENT, this.onSideBarToggle);
			if(this.doInit){
				this.doInit(args);
				this.currentURL=window.location.hash;
			}
			if ($.cookie('offcanvas') == 'hide') {
				$('#content-wrapper').addClass('no-transition');
				$('#sidebar-wrapper').hide();
				$('#sidebar-wrapper').css('right', -($('#sidebar-wrapper').outerWidth() + 10));
				$('#content-wrapper').removeClass('col-md-10').addClass('col-md-12');
			}
			else if ($.cookie('offcanvas') == 'show') {
				$('#sidebar-wrapper').show(500).animate({ right: 0 });
				//  $('#sidebar-wrapper').show();
				$('#content-wrapper').removeClass('no-transition');
				$('#content-wrapper').removeClass('col-md-12').addClass('col-md-10');
			}

			$(window).on('resize', this.onWindowResize);
			$('#content-wrapper').css('padding-top','60px');
			$('#notification-btn').unbind().on('click', this.hideOrDisplaySideBar);
			$('.notifyDetail').unbind().on('click', this.hideOrDisplaySideBar);
			/*$('#notification-btn').on('shown.bs.dropdown', function(){
				$('#notification-btn').find('.dbmgr-notify-icon').remove();
			});*/
		},
		hideOrDisplaySideBar: function(args){
			$('#notification-btn').find('.dbmgr-notify-icon').remove();
			$('#content-wrapper').removeClass('no-transition');
			if ($('#sidebar-wrapper').is(':visible') && $('#content-wrapper').hasClass('col-md-10')) {
				// Slide out
				$('#sidebar-wrapper').animate({
					right: -($('#sidebar-wrapper').outerWidth() + 10)
				}, function () {
					$('#sidebar-wrapper').hide(1000);
				});
				$('#content-wrapper').removeClass('col-md-10').addClass('col-md-12');
				$.cookie('offcanvas', 'hide');
			}
			else {
				// Slide in
				$('#sidebar-wrapper').show(500).animate({ right: 0 });
				$('#content-wrapper').removeClass('col-md-12').addClass('col-md-10');
				$.cookie('offcanvas', 'show');
			}
			if($('#content-wrapper').hasClass('col-md-12') && $('#sidebar-wrapper').is(':hidden')) {
				$('#sidebar-wrapper').animate({
					right: 0
				}, function () {
					$('#sidebar-wrapper').show(1000);
				});
				//  $('#sidebar-wrapper').show();
				$('#content-wrapper').removeClass('no-transition');
				$('#content-wrapper').removeClass('col-md-12').addClass('col-md-10');
			}	
			common.fire(common.SIDEBAR_TOGGLE_EVENT,'');
		},
		resume: function(args){
			if(this.doResume){
				this.doResume(args);
				this.currentURL=window.location.hash;
			}
			$(window).on('resize', this.onWindowResize);
			common.on(common.SIDEBAR_TOGGLE_EVENT, this.onSideBarToggle);
			common.on(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
			$('.notifyDetail').unbind().on('click', this.hideOrDisplaySideBar);
		},
		pause: function() {
			if($('#sidebar-wrapper').is(':visible')){
				$('#content-wrapper').removeClass('col-md-10').addClass('col-md-12');
				$.cookie('offcanvas', 'hide');
				$('#sidebar-wrapper').hide();
			}
			common.off(common.SIDEBAR_TOGGLE_EVENT, this.onSideBarToggle);
			$(window).off('resize', this.onWindowResize);
			$('.notifyDetail').off('click', this.hideOrDisplaySideBar);
			if(this.doPause){
				this.doPause();
			}
			common.off(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
		},
		openCloseMessage:function(event,index){
			var child=$(this.nextSibling);
			if (child.is(":visible")) {
	            // This row is already open - close it
				child.hide();
	            $(this).removeClass('shown');
	        }
	        else {
	            // Open this row
	        	child.show();
	        	$(this).addClass('shown');
	        }
			event.stopPropagation();
		},
		refreshTimeDiff:function(){
			var current = new Date();
			for(var i=0;i<common.MESSAGE_LIST.length;i++){
				var timeAgo = common.toTimeDifferenceFromLocalDate(common.MESSAGE_LIST[i].time,current);
				$("span.timeAgo").eq(i).text(timeAgo);
			}
		},
		collectNewNotifyMessage:function(obj){
			var notifyIndicator = $('#notification-btn').find('.dbmgr-notify-icon');
			if(notifyIndicator.length == 0&& $('#content-wrapper').hasClass('col-md-12') && $('#sidebar-wrapper').is(':hidden')){
				$('#notification-btn').append('<i class="dbmgr-notify-icon fa fa-exclamation-circle fa-stack-1x" style="color:yellow"></i>');
			}

			var currentTime=new Date();
			setInterval(__this.refreshTimeDiff,60000);
			/*var interval=setInterval(__this.flashBackground,1000);
			setTimeout(function(){clearInterval(interval)},5000);*/
			//remove the empty li.
			if($("#notifyMenu>ul").text().trim()=='No available notifications'){
				$("#notifyMenu>ul").remove();
			}
			if(obj.msg == undefined){
				obj.msg="there is no response for current request.";
				obj.shortMsg="there is no response.";
			}
			var alertClass = "alert-success fa-check-circle";
			if(obj.tag == "danger"){
				alertClass = "alert-danger fa-times-circle";
			}else if(obj.tag == "warning"){
				alertClass = "alert-warning fa-warning";

			}

			//Compute the hash string on the url.
			var hs = common.hashString(obj.url ? obj.url : "");

			if(obj.lastMessageOnly && obj.lastMessageOnly == true){

				//delete all other earlier messages that match obj.url
				$.each(common.MESSAGE_LIST, function (index , value){
					if(obj.url == value.url){
						common.MESSAGE_LIST.splice(index, 1);
						common.MESSAGE_COUNT --;
					}
				});

				//Delete the older messages from the UI, by matching the hash string
				var itemList = $('#notifyMenu>ul.odd');
				if(itemList.length > 0){
					for(var i=0;i<itemList.length;i++){
						if($(itemList[i]+">li").attr("hashstr") && $(itemList[i]).attr("hashstr") == hs){
							$(itemList[i]+">li").remove();
							if($(itemList[i+1]+">li").hasClass("divider")){
								$(itemList[i+1]+">li").remove();
								i++;								
							}
						}
					}
				}
			}

			/*$("#notifyMenu").prepend('<li hashstr="'+hs+'"><a class="active"><div class="notifyDetail"><i class="fa '+ alertClass + ' fa-fw"></i><i style="padding-left:2px">'+ obj.shortMsg.substr(0,35)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¯ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â½</button></div></a></li><li class="divider"></li>');*/
			if(obj.url!=null){
				$("#notifyMenu")
				.prepend(
						'<ul class="odd" style="padding-left: 30px;margin-bottom:0px;margin-top:6px"><li hashstr="'+hs+'" style="list-style: none">'
						+ '<div class="notifyDetail">'
						+ '<i class="fa '+ alertClass + ' fa-fw"></i>'
						+ '<i style="padding-left: 2px">'+ obj.shortMsg.substr(0,35)+'</i><br/> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span>'
						+ '<button type="button" aria-hidden="true"'
						+ 'class="pull-right close" data-notify="dismiss"'
						+ 'style="color: black;font-size:small;margin-right:10px">x</button>'
						+ '</div></li></ul>'
						+ '<ul class="even" style="padding-left: 30px;display:none">'
						+ '<li style="list-style: none"><a class="active">'
						+ '<div class="notifyDetail">'
						+ '<i style="padding-left: 2px;"><a  href='+ obj.url +' style="color:black;font-size:small">'+obj.msg+'</a></i>'
						+ '</div></a></li></ul><hr style="margin-top:6px;margin-bottom:0px"/>');							
			}else{
				$("#notifyMenu")
				.prepend(
						'<ul class="odd" style="padding-left: 30px;margin-bottom:0px;margin-top:6px"><li hashstr="'+hs+'" style="list-style: none">'
						+ '<div class="notifyDetail">'
						+ '<i class="fa '+ alertClass + ' fa-fw"></i>'
						+ '<i style="padding-left: 2px">'+ obj.shortMsg.substr(0,35)+'</i><br/> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span>'
						+ '<button type="button" aria-hidden="true"'
						+ 'class="pull-right close" data-notify="dismiss"'
						+ 'style="color: black;font-size:small;margin-right:10px">x</button>'
						+ '</div></li></ul>'
						+ '<ul class="even" style="padding-left: 30px;display:none">'
						+ '<li hashstr="" style="list-style: none"><a class="active">'
						+ '<div class="notifyDetail">'
						+ '<i style="padding-left: 2px;color:black;font-size:small">'+obj.msg+'</i>'
						+ '</div></a></li></ul><hr style="margin-top:6px;margin-bottom:0px"/>');	
			}
			common.MESSAGE_LIST.splice(0,0,{msg:obj.msg,tag:obj.tag,url:obj.url,time:currentTime});
			$('#notifyMenu>ul.odd').unbind().on("click",_this.openCloseMessage);
			$(".close").unbind().on("click",__this.removeNotificationMessage);
		},
		removeNotificationMessage:function(event,index){
			var i;
			if($.type(event)=="object"){
				//triggered by the panel x mark.
				i=$(this).closest("ul").index();
			}/*else{
				//triggered by the popup x mark waiting for plugin bower download available of onclick function..
				i=index;
			}*/
			$("#notifyMenu>ul").eq((2*i/3)).remove();
			$("#notifyMenu>ul").eq((2*i/3)).remove();
			$("hr").eq(i/3).remove();
			event.stopPropagation();
		},
		popupNotificationMessage:function(event,obj){
			var message=null;
			/*if(event!=null){
				//this for redirect case
				var i=$(this).closest("li").index();
				event.stopPropagation();
				//used to remove the notification from popup.
				common.popupIndex=(i-1)/2;
				obj=common.MESSAGE_LIST[(i-1)/2];
			}*/
			//this is for no redirect
			if(obj.msg==undefined){
				obj.msg="Operation failed.";
			}
			if(obj.url!=null){
				message='<a  href='+ obj.url +' style="color:#ffffff">'+obj.msg+'</a>';
			}else{
				message=obj.msg;
			}
			$.notify({message: message,
				target:'_self'},
				{
					// settings
					element: 'body',
					type:obj.tag,
					z_index:1000,
					delay:10000,
					timer:1000,
					/*onClose:__this.removeNotificationFromPopup,*/
					placement: {
						from: 'bottom',
						align: 'right'
					}});
		},
		removeNotificationFromPopup:function(){
			/*__this.removeNotificationMessage(common.popupIndex*2);*/
		},
		remove: function(){
			_this.refreshTimer.stop();
			this.pause();
			var childElement = $(this.$el[0]).find('#page-wrapper');
			if(childElement && childElement.length > 0)
				this.pageWrapper = childElement.detach();

		},

		reset: function(){
			if(this.doReset){
				this.doReset();
			}
		},
		onWindowResize: function() {
			clearTimeout(resizeTimer);
			resizeTimer = setTimeout(_this.doWindowResize, 300);
		},
		doWindowResize: function() {
			if(_this.handleWindowResize){
				_this.handleWindowResize();
			}
		},
		onSideBarToggle: function(){
			clearTimeout(sidebarTimer);
			sidebarTimer = setTimeout(_this.doSideBarToggle, 500);

		},
		doSideBarToggle:function(){
			if(_this.handleSideBarToggle){
				_this.handleSideBarToggle();
			}			
		}
	});

	return BaseView;
});
