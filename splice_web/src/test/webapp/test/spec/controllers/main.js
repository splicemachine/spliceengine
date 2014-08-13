'use strict';

// Describe the test suite.
describe('Controller: mainController', function () {

	// Load the controller's module.
	beforeEach(module('spliceAdminControllers'));

	var mainController, scope;

	// Initialize the controller and a mock scope.
	beforeEach(inject(function ($controller, $rootScope) {
		scope = $rootScope.$new();
		mainController = $controller('mainController', {
			$scope: scope
		});
	}));

	it('should set the homeTab as "active" on the scope', function () {
		expect(scope.homeTab).toBe('active');
	});
});
