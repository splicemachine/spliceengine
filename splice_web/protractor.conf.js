exports.config = {
	allScriptsTimeout: 11000,

	specs: [
		'src/test/webapp/test/e2e/**/*-scenarios.js'
	],

	capabilities: {
		'browserName': 'chrome'
	},

	chromeOnly: true,

	baseUrl: 'http://localhost:7020/',

	framework: 'jasmine',

	jasmineNodeOpts: {
		defaultTimeoutInterval: 30000
	}
};
