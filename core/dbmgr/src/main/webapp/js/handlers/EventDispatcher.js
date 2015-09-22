define([], function() { //"use strict";

	var EventDispatcher = (function() {
		function EventDispatcher() {

			var events = {};

			this.on = function (eventName, callback) {
				if (!events.hasOwnProperty(eventName)) {
					events[eventName] = [];
				}
				events[eventName].push(callback);
			};

			this.off = function (eventName, callback) {
				if (events.hasOwnProperty(eventName)) {
					events[eventName] = $.grep(events[eventName], function (func) {
						return (callback !== func);
					});
				}
			};

			// fire an event
			this.fire = function (eventName, eventInfo) {
				var i, fireEvents;

				if (events.hasOwnProperty(eventName)) {
					fireEvents = events[eventName];

					for (i = 0; i < fireEvents.length; i++) {
						fireEvents[i](eventInfo);
					}
				}
			};

			this.getEvents = function() {
				return events;
			};
		}

		return EventDispatcher;
	}());
	return EventDispatcher;
});
