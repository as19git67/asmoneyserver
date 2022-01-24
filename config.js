const nconf = require('nconf');

nconf.argv().env().file({file: 'settings.json'});

nconf.defaults({
  "httpPort": 3000,
  "dbhost": "127.0.0.1",
  "dbname": "finanzkraft",
  "dbuser": "somebody",
  "dbuserpass": "secret",
  "dbdebug": false,
  "adminUser": "",
  "initialAdminPassword": "secret",
  "tokenLifetime": '600'
});

module.exports = nconf;
