// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

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
	var BaseView = Backbone.View.extend({

		el: $('#wrapper'),

		initialized: false,
		
		TASK_MENU:'#notify',

		pageWrapper: null,
		
		currentURL:null,
		
		isAllNotificationInserted:false,
		
		hideNotifications:null,

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
			if(this.doInit){
				this.doInit(args);
				this.currentURL=window.location.hash;
			}
		},
		resume: function(args){
			if(this.doResume){
				this.doResume(args);
				this.currentURL=window.location.hash;
			}
			common.on(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
		},
		pause: function() {
			if(this.doPause){
				this.doPause();
			}
			common.off(common.NOFITY_MESSAGE, this.collectNewNotifyMessage);
		},
		flashBackground:function(){
			$("#notifyMenu>a").attr("style","background-color:yellow");
			setTimeout(function(){$("#notifyMenu>a").attr("style","background-color:transparent");},500);
		},
		refreshTimeDiff:function(){
			var current = new Date();
			var diffList=new Array();
			for(var i=0;i<common.MESSAGE_LIST.length;i++){
				var diff=current-common.MESSAGE_LIST[i].time;
				var timeAgo=null;
				if(1<diff/(60*1000)<60){
					timeAgo=(diff/(60*1000)).toFixed(0) +' minutes ago';
				}else if(diff/(24*3600*1000)<1){
					timeAgo=(diff/(3600*1000)).toFixed(0) +' hours ago';
				}else if(diff/(7*24*3600*1000)<1){
					timeAgo=(diff/(24*3600*1000)).toFixed(0) +' days ago';
				}else if(diff/(30*7*24*3600*1000)<1){
					timeAgo=(diff/(7*24*3600*1000)).toFixed(0) +' weeks ago';
				}else if(diff/(12*30*7*24*1000)<1){
					timeAgo=(diff/(30*7*24*1000)).toFixed(0) +' months ago';
				}else{
					timeAgo=(diff/(12*30*7*24*1000)).toFixed(0) +' years ago';
				}
				$("span.timeAgo").eq(i).text(timeAgo);
			}
		},
		collectNewNotifyMessage:function(obj){
			common.MESSAGE_COUNT++;
			var currentTime=new Date();
			var interval=setInterval(__this.flashBackground,1000);
			setInterval(__this.refreshTimeDiff,60000);
			setTimeout(function(){clearInterval(interval)},5000);
			//remove the empty li.
			if($("#notifyMenu>ul>li>a>strong").text().trim()=='empty'){
				$("#notifyMenu>ul>li").remove();			
			}
			//check whether there is number already.
			if($("#notifyMenu>a>i>small").text().length!=0){
				$("#notifyMenu>a>i>small").text(common.MESSAGE_COUNT);
			}else{
				$("#notifyMenu>a>i").eq(0).append("<small>"+common.MESSAGE_COUNT+"</small>");
			}
			//sometimes the response will be undefined.
			if(common.MESSAGE_COUNT>4){
				if(_this.isAllNotificationInserted==false){
					_this.isAllNotificationInserted=true;
					$("#notifyMenu>ul").append('<li id="allNotification"><a class="text-center active"><strong>See All Notifications</strong><i class="fa fa-angle-right"></i></a></li>');
				}
				if(obj.msg!=undefined){
									$("#notifyMenu>ul").prepend('<li style="display: none;"><a class="active"><div class="notifyDetail"><i class="fa fa-tasks fa-fw"></i><i>'+ obj.msg.substr(0,20)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">×</button></div></a></li><li class="divider" style="display: none;"></li>');
								}else{
									obj.msg="there is no response for current request!";
									$("#notifyMenu>ul").prepend('<li style="display: none;"><a class="active"><div  class="notifyDetail"><i class="fa fa-tasks fa-fw"></i><i>'+ obj.msg.substr(0,20)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">×</button></div></a></li><li class="divider" style="display: none;"></li>');
								}
			}else{
				if(obj.msg!=undefined){
					
					$("#notifyMenu>ul").prepend('<li><a class="active"><div class="notifyDetail"><i class="fa fa-tasks fa-fw"></i><i">'+ obj.msg.substr(0,20)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">×</button></div></a></li><li class="divider"></li>');
				}else{
					obj.msg="there is no response for current request!";
					$("#notifyMenu>ul").prepend('<li><a class="active"><div class="notifyDetail"><i class="fa fa-tasks fa-fw"></i><i">'+ obj.msg.substr(0,20)+'</i> <span class="text-muted small timeAgo" style="margin-left: 5px;"> 0 minutes ago </span><button type="button" aria-hidden="true" class="pull-right close" data-notify="dismiss" style="color: black;">×</button></div></a></li><li class="divider"></li>');
				}
			}
			common.MESSAGE_LIST.splice(0,0,{msg:obj.msg,tag:obj.tag,url:obj.url,time:currentTime});
			$(".notifyDetail").unbind().on("click",__this.popupNotificationMessage);
			$(".close").unbind().on("click",__this.removeNotificationMessage);
			$("#allNotification").unbind().on("click",__this.showAllNotifications);
		},
		showAllNotifications:function(){
			/*$("#notifyMenu").attr("class","dropdown active open");*/
			_this.hideNotifications= $("#notifyMenu>ul>li[style='display: none;']");
			$("#notifyMenu>ul>li").show();
			$("#allNotification>a>strong").text("See Part of Notifications");
			$("#allNotification").unbind().on("click",__this.hidePartOfNotifications);
		},
		hidePartOfNotifications:function(){
			/*$("#notifyMenu").attr("class","dropdown active open");*/
			_this.hideNotifications.hide();
			$("#allNotification>a>strong").text("See All Notifications");
			$("#allNotification").unbind().on("click",__this.showAllNotifications);
		},
		removeNotificationMessage:function(index){
			var i;
			if($.type(index)=="object"){
				//triggered by the panel x mark.
				i=$(this).closest("li").index();
			}else{
				//triggered by the popup x mark.
				i=index;
			}
			common.MESSAGE_COUNT--;
			$("#notifyMenu>a>i>small").text(common.MESSAGE_COUNT);
			$("#notifyMenu>ul>li").eq(i).remove();
			$("#notifyMenu>ul>li").eq(i).remove();
			common.MESSAGE_LIST.splice(common.MESSAGE_LIST[i/2],1);
			//if the removed element is display:block, redefine the hide and show notifications
			if(this.parentElement.style.display==""){
				__this.redefineShowHideNotifications();
			}
			if($("#notifyMenu>ul>li [class!='divider']").length==0){
				$("#notifyMenu>ul").prepend('<li><a class="text-center active" href="#"><i class="fa fa-angle-left"></i><strong>empty </strong><i class="fa fa-angle-right"></i></a></li>');
			}
		},
		redefineShowHideNotifications:function(){
			//remove "all notification" li
			var elementList=$("#notifyMenu>ul>li");
			var length=elementList.length;
			var showList=elementList.slice(length-9);
			//get the four show elements
			showList.show();
			_this.hideNotifications = elementList.splice(length-9,9);
		},
		popupNotificationMessage:function(obj){
			if(obj.msg==undefined){
				//this for redirect case
				var i=$(this).closest("li").index();
				//used to remove the notification from popup.
				common.popupIndex=i/2;
				obj=common.MESSAGE_LIST[i/2];
			}
			//this is for no redirect
			$.notify({message: obj.msg,
				url:obj.url,
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
			__this.removeNotificationMessage(common.popupIndex*2);
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
		}
	});

	return BaseView;
});
