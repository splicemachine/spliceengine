'use strict';
module.exports = function(grunt) {

  // load all grunt tasks automatically
  require('load-grunt-tasks')(grunt);

  var mountFolder = function(connect, dir) {
    return connect.static(require('path').resolve(dir));
  };

  var cfg = {
    srcDir: 'source',
    buildDir: 'dist',
    demoDir: 'demo'
  };

  // project configuration
  grunt.initConfig({
    cfg: cfg,

    // watch
    watch: {
      livereload: {
        files: [
          '<%= cfg.demoDir %>/**/*.js',
          '<%= cfg.demoDir %>/**/*.css',
          '<%= cfg.demoDir %>/**/*.html',
          '!<%= cfg.buildDir %>/*.js',
          '!<%= cfg.demoDir %>/dist/*.js',
          '!<%= cfg.demoDir %>/bower_components/**/*'
        ],
        options: {
          livereload: true
        }
      },
      build: {
        files: [
          '<%= cfg.srcDir %>/**/*.js',
          '!<%= cfg.buildDir %>/*.js'
        ],
        tasks: ['jshint:source', 'clean:build', 'concat:build', 'uglify:build', 'cssmin', 'copy']
      },
      cssmin: {
        files: [
          '<%= cfg.srcDir %>/**/*.css'
        ],
        tasks: ['cssmin', 'copy']
      }
    },

    // clean up files as part of other tasks
    clean: {
      build: {
        src: ['<%= cfg.buildDir %>/**']
      },
      demo: {
        src: ['<%= cfg.demoDir %>/dist/**']
      }
    },

    // prepare files for demo
    copy: {
      main: {
        files: [{
          expand: true,
          src: ['<%= cfg.buildDir %>/*.*'],
          dest: '<%= cfg.demoDir %>/'
        }]
      }
    },

    jshint: {
      options: {
        'jshintrc': true,
        reporter: require('jshint-stylish')
      },
      source: {
        files: {
          src: ['<%= cfg.srcDir %>/**/*.js']
        }
      },
      demo: {
        files: {
          src: [
            '<%= cfg.demoDir %>/**/*.js',
            '!<%= cfg.demoDir %>/bower_components/**/*'
          ]
        }
      }
    },

    // concat
    concat: {
      build: {
        src: [
          '<%= cfg.srcDir %>/main.js',
          '<%= cfg.srcDir %>/services/helper.js',
          '<%= cfg.srcDir %>/controllers/treeCtrl.js',
          '<%= cfg.srcDir %>/controllers/nodesCtrl.js',
          '<%= cfg.srcDir %>/controllers/nodeCtrl.js',
          '<%= cfg.srcDir %>/controllers/handleCtrl.js',
          '<%= cfg.srcDir %>/directives/uiTree.js',
          '<%= cfg.srcDir %>/directives/uiTreeNodes.js',
          '<%= cfg.srcDir %>/directives/uiTreeNode.js',
          '<%= cfg.srcDir %>/directives/uiTreeHandle.js',
        ],
        dest: '<%= cfg.buildDir %>/angular-ui-tree.js'
      }
    },

    // uglify
    uglify: {
      options: {
        preserveComments: 'some',
        mangle: false
      },
      build: {
        files: {
          '<%= cfg.buildDir %>/angular-ui-tree.min.js': ['<%= cfg.buildDir %>/angular-ui-tree.js']
        }
      }
    },

    // connect
    connect: {
      options: {
        port: 9000,
        livereload: 35729,
        hostname: '0.0.0.0'
      },
      demo: {
        options: {
          middleware: function(connect) {
            return [
              mountFolder(connect, '')
            ];
          }
        }
      }
    },

    cssmin: {
      add_banner: {
        options: {
          banner: '/* angular-ui-tree css file */'
        },
        files: {
          '<%= cfg.buildDir %>/angular-ui-tree.min.css': ['<%= cfg.srcDir %>/angular-ui-tree.css']
        }
      }
    },

    // open
    open: {
      server: {
        path: 'http://localhost:<%= connect.options.port %>/<%= cfg.demoDir %>/'
      }
    },

    // karma
    karma: {
      options: {
        configFile: 'karma.conf.js',
        autoWatch: true
      },

      single: {
        singleRun: true,
        browsers: ['PhantomJS']
      },

      continuous: {
        singleRun: false,
        browsers: ['PhantomJS', 'Firefox']
      }
    },

    // available tasks
    tasks_list: {
      options: {},
      project: {
        options: {
          tasks: [{
            name: 'build',
            info: 'Create a build of (tested) the source files'
          }, {
            name: 'webserver',
            info: 'Build the project, watch filechanges and start a webserver'
          }, {
            name: 'test',
            info: 'Runt tests'
          }, {
            name: 'test:continuous',
            info: 'Runt tests continuously'
          }]
        }
      }
    },

    ngdocs: {
      options: {
        dest: 'demo/docs',
        scripts: ['angular.js'],
        html5Mode: true,
        startPage: '/guide',
        title: 'angular-ui-tree',
        analytics: {
          account: 'UA-48778560-1',
          domainName: 'github.io'
        }
      },
      guide: {
        src: ['guide/*.ngdoc'],
        title: 'Guide'
      },
      api: {
        src: ['source/**/*.js', '!src/**/*.spec.js'],
        title: 'API Documentation'
      }
    }
  });

  // default
  grunt.registerTask('default', ['tasks_list:project']);
  grunt.registerTask('build', ['jshint:source', 'clean:build', 'concat:build', 'cssmin', 'uglify:build', 'copy']);
  grunt.registerTask('webserver', ['build', 'open', 'connect:demo', 'watch']);
  grunt.registerTask('test', ['karma:single']);
  grunt.registerTask('test:continuous', ['karma:continuous']);
};