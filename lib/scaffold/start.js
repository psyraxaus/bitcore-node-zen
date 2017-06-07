'use strict';

var path = require('path');
var BitcoreNode = require('../node');
var index = require('../');
var bitcore = require('bitcore-lib-zen');
var _ = bitcore.deps._;
var log = index.log;
var shuttingDown = false;
var fs = require('fs');

log.debug = function() {};

/**
 * Checks for configuration options from version 2. This includes an "address" and
 * "db" service, or having "datadir" at the root of the config.
 */
function checkConfigVersion2(fullConfig) {
  var datadirUndefined = _.isUndefined(fullConfig.datadir);
  var addressDefined = (fullConfig.services.indexOf('address') >= 0);
  var dbDefined = (fullConfig.services.indexOf('db') >= 0);

  if (!datadirUndefined || addressDefined || dbDefined) {

    console.warn('\nConfiguration file is not compatible with this version. \n' +
                 'A reindex for zend is necessary for this upgrade with the "reindex=1" zen.conf option. \n' +
                 'There are changes necessary in both zen.conf and bitcore-node.json. \n\n' +
                 'To upgrade please see the details below and documentation at: \n' +
                 'https://github.com/bitpay/bitcore-node/blob/bitcoind/docs/upgrade.md \n');

    if (!datadirUndefined) {
      console.warn('Please remove "datadir" and add it to the config at ' + fullConfig.path + ' with:');
      var missingConfig = {
        servicesConfig: {
          bitcoind: {
            spawn: {
              datadir: fullConfig.datadir,
              exec: path.resolve(__dirname, '../../bin/zend')
            }
          }
        }
      };
      console.warn(JSON.stringify(missingConfig, null, 2) + '\n');
    }

    if (addressDefined || dbDefined) {
      console.warn('Please remove "address" and/or "db" from "services" in: ' + fullConfig.path + '\n');
    }

    return true;
  }

  return false;
}

/**
 * This function will instantiate and start a Node, requiring the necessary service
 * modules, and registering event handlers.
 * @param {Object} options
 * @param {Object} options.servicesPath - The path to the location of service modules
 * @param {String} options.path - The absolute path of the configuration file
 * @param {Object} options.config - The parsed bitcore-node.json configuration file
 * @param {Array}  options.config.services - An array of services names.
 * @param {Object} options.config.servicesConfig - Parameters to pass to each service
 * @param {String} options.config.network - 'livenet', 'testnet' or 'regtest
 * @param {Number} options.config.port - The port to use for the web service
 */
function start(options) {

  var fullConfig = _.clone(options.config);

  var servicesPath;
  if (options.servicesPath) {
    servicesPath = options.servicesPath;
  } else {
    servicesPath = options.path;
  }

  fullConfig.path = path.resolve(options.path, './bitcore-node.json');

  fullConfig.services = start.setupServices(require, servicesPath, options.config);

  var node = new BitcoreNode(fullConfig);

  // setup handlers for uncaught exceptions and ctrl+c
  start.registerExitHandlers(process, node);

  node.on('ready', function() {
    log.info('Bitcore Node ready');
  });

  node.on('error', function(err) {
    log.error(err);
  });

  node.start(function(err) {
    if(err) {
      log.error('Failed to start services');
      if (err.stack) {
        log.error(err.stack);
      }
      start.cleanShutdown(process, node);
    }
  });

  return node;

}

/**
 * Checks a service for the expected methods
 * @param {Object} service
 */
function checkService(service) {
  if (!service.module.prototype ||
      !service.module.dependencies ||
      !service.module.prototype.start ||
      !service.module.prototype.stop) {
    throw new Error(
      'Could not load service "' +
        service.name +
        '" as it does not support necessary methods and properties.');
  }
}

function lookInRequirePathConfig(req, service) {
  if (!service.config.requirePath) {
    return;
  }

  try {
    if (fs.statSync(service.config.requirePath).isDirectory()) {
      return req(service.config.requirePath);
    }
    var serviceFile = service.config.requirePath.replace(/.js$/, '');
    return req(serviceFile);
  } catch(e) {
    log.info('Checked the service\'s requirePath value, ' +
      'but could not find the service, checking elsewhere. ' +
        'Error caught: ' + e.message);
  }
}

function lookInCwd(req, service) {
  var location = service.config.cwdRequirePath ? service.config.cwdRequirePath : service.name;
  try {
    return req(process.cwd() + '/' + location);
  } catch(e) {
    if(e.code !== 'MODULE_NOT_FOUND') {
      log.error(e);
    }
    log.info('Checked the current working directory for service: ' + location);
  }
}

function lookInBuiltInPath(req, service) {
  try {
    var serviceFile = path.resolve(__dirname, '../services/' + service.name);
    return req(serviceFile);
  } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') {
      log.error(e);
    }
    log.info('Checked the built-in path: lib/services, for service: ' + service.name);
  }
}

function lookInModuleManifest(req, service) {
  try {
    var servicePackage = req(service.name + '/package.json');
    var serviceModule = service.name;
    if (servicePackage.bitcoreNode) {
      serviceModule = serviceModule + '/' + servicePackage.bitcoreNode;
      return req(serviceModule);
    }
  } catch(e) {
    log.info('Checked the module\'s package.json for service: ' + service.name);
  }
}

function loadModule(req, service) {
  var serviceCode;

  //first, if we have explicitly set the require path for our service:
  serviceCode = lookInRequirePathConfig(req, service);

  //second, look in the current working directory (of the controlling terminal, if there is one) for the service code
  if(!serviceCode) {
    serviceCode = lookInCwd(req, service);
  }

  //third, try the built-in services
  if(!serviceCode) {
    serviceCode = lookInBuiltInPath(req, service);
  }

  //fourth, see if there is directory in our module search path that has a
  //package.json file, if so, then see if there is a bitcoreNode field, if so
  //use this as the path to the service module
  if(!serviceCode) {
    serviceCode = lookInModuleManifest(req, service);
  }

  if (!serviceCode) {
    throw new Error('Attempted to load the ' + service.name + ' service from: ' +
      'the requirePath in the services\' config, then "' +
      process.cwd() + '" then from: "' + __dirname + '/../lib/services' + '" finally from: "' +
      process.cwd() + '/package.json" - bitcoreNode field. All paths failed to find valid nodeJS code.');
  }

  service.module = serviceCode;
}

/**
 * This function will loop over the configuration for services and require the
 * specified modules, and assemble an array in this format:
 * [
 *   {
 *     name: 'bitcoind',
 *     config: {},
 *     module: BitcoinService
 *   }
 * ]
 * @param {Function} req - The require function to use
 * @param {Array} servicesPath - The local path (for requiring services)
 * @param {Object} config
 * @param {Array} config.services - An array of strings of service names.
 * @returns {Array}
 */
function setupServices(req, servicesPath, config) {

  module.paths.push(path.resolve(servicesPath, './node_modules'));

  var services = [];
  if (config.services) {
    for (var i = 0; i < config.services.length; i++) {
      var service = {};

      service.name = config.services[i];

      var hasConfig = config.servicesConfig && config.servicesConfig[service.name];
      service.config = hasConfig ? config.servicesConfig[service.name] : {};

      loadModule(req, service);
      checkService(service);

      services.push(service);
    }
  }
  return services;
}

function cleanShutdown(_process, node) {
  node.stop(function(err) {
    if(err) {
      log.error('Failed to stop services: ' + err);
      return _process.exit(1);
    }
    log.info('Halted');
    _process.exit(0);
  });
}

function exitHandler(options, _process, node, err) {
  if (err) {
    log.error('uncaught exception:', err);
    if(err.stack) {
      log.error(err.stack);
    }
    node.stop(function(err) {
      if(err) {
        log.error('Failed to stop services: ' + err);
      }
      _process.exit(-1);
    });
  }
  if (options.sigint) {
    if (!shuttingDown) {
      shuttingDown = true;
      start.cleanShutdown(_process, node);
    }
  }
}

function registerExitHandlers(_process, node) {
  _process.on('uncaughtException', exitHandler.bind(null, {exit:true}, _process, node));
  _process.on('SIGINT', exitHandler.bind(null, {sigint:true}, _process, node));
}

module.exports = start;
module.exports.registerExitHandlers = registerExitHandlers;
module.exports.exitHandler = exitHandler;
module.exports.setupServices = setupServices;
module.exports.cleanShutdown = cleanShutdown;
