'use strict';

/**
 * Suite of e2e tests for index.html view.
 */

describe('View: index.html', function() {

	browser.get('/app/');
	it('should automatically redirect to /traced-statements when location hash/fragment is empty', function() {
		expect(browser.getLocationAbsUrl()).toMatch("/traced-statements");
	});

	describe('home', function() {
		beforeEach(function() {
			browser.get('/app/#/home');
		});
		it('should render traced-statements when user navigates to /home', function() {
			expect(element.all(by.css('[ng-view] h4')).first().getText()).
				toMatch(/Explain Traces/);
		});
	});

	describe('traced-statements', function() {
		beforeEach(function() {
			browser.get('/app/#/traced-statements');
		});
		it('should render traced-statements when user navigates to /traced-statements', function() {
			expect(element.all(by.css('[ng-view] h4')).first().getText()).
				toMatch(/Explain Traces/);
		});
	});

	describe('contact', function() {
		beforeEach(function() {
			browser.get('/app/#/contact');
		});
		it('should render contact when user navigates to /contact', function() {
			expect(element.all(by.css('[ng-view] h4')).first().getText()).
				toMatch(/Documentation/);
		});
	});
});
