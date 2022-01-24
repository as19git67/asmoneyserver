const config = require('./config');
const DB = require('./database');
const Users = require('./Users');
const _ = require('lodash');

module.exports = function (app, callback) {
  let adminUser = config.get('adminUser');
  if (!adminUser) {
    console.log("Not checking if initialization needed, because adminUser is not configured");
    if (_.isFunction(callback)) {
      callback();
    }
    return;
  }

  let database = new DB();

  database.isSchemaOK(function (err, exists) {
    if (err) {
      console.log("database.isSchemaOK failed", err);
      if (_.isFunction(callback)) {
        callback(err);
      }
    } else {
      if (exists) {
        console.log("Database schema is ok. Not performing initial config.");
        if (_.isFunction(callback)) {
          callback();
        }
      } else {
        const initialAdminPassword = config.get('initialAdminPassword');
        if (initialAdminPassword) {
          database.makeSchemaUpToDate(function (err) {
            if (err) {
              console.log("ERROR: Creating or upgrading database schema failed: ", err);
              if (_.isFunction(callback)) {
                callback(err);
              }
            } else {
              const finanzkraftUserConfig = config.get('finanzkraftUser');
              let users = [];
              if (finanzkraftUserConfig) {
                _.each(finanzkraftUserConfig.split(';'), function (userKeyVal) {
                  let userPassArray = userKeyVal.split('=');
                  if (userPassArray.length === 2) {
                    let userPass = {user: userPassArray[0].trim(), pass: userPassArray[1].trim()};
                    if (userPass.user && userPass.pass) {
                      users.push(userPass);
                    }
                  }
                });
              }

              async.eachSeries(users, function (userPass, cb) {

                new Users().createUser(userPass.user, userPass.pass, function (err) {
                  if (err) {
                    console.log("ERROR creating user " + userPass.user, err);
                    cb(err);
                  } else {
                    cb();
                  }
                })

              }, function (err) {
                if (err) {
                  // One of the iterations produced an error.
                  // All processing will now stop.
                  console.log('Failed to create finanzkraft users');
                  if (_.isFunction(callback)) {
                    callback(err);
                  }
                } else {
                  console.log('All finanzkraft users have been created successfully');
                  console.log("Database schema is now up to date");
                  if (_.isFunction(callback)) {
                    callback();
                  }
                }
              });
            }
          });
        } else {
          const errMsg = "ERROR: Not creating or upgrading database schema, because initialAdminPassword is not configured";
          console.log(errMsg);
          if (_.isFunction(callback)) {
            callback(errMsg);
          }
        }
      }
    }
  });

};