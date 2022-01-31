const _ = require('lodash');
const moment = require('moment');
const BearerStrategy = require('passport-http-bearer').Strategy;
const BasicStrategy = require('passport-http').BasicStrategy;
const config = require('./config');
const Users = require('./Users');

module.exports.users = new Users();

let notRegisteredUsers = {};

module.exports.init = function (passport, callback) {
  let enabledStrategies = {};

  // Passport session setup.
  //   To support persistent login sessions, Passport needs to be able to
  //   serialize users into and deserialize users out of the session.  Typically,
  //   this will be as simple as storing the user ID when serializing, and finding
  //   the user by ID when deserializing.
  passport.serializeUser(function (user, done) {
    if (user.isNotLocalUser) {
      console.log('serialize notRegisteredUser with id ' + user.id);
      notRegisteredUsers[user.id] = user;
      done(null, {
        isNotLocalUser: true,
        providerAndKey: user.id,
        provider: user.profile.provider,
        providerKey: user.profile.id
      });
    } else {
      // nothing to do registered user - remembering the id is enough
      console.log('serialize user with id ' + user.id);
      done(null, {isNotLocalUser: false, id: user.id});
    }
  });

  passport.deserializeUser(function (userSpec, done) {
    findUser(userSpec, function (err, user) {
      if (!err && user && user.id) {
        if (user.isNotLocalUser) {
          done(err, user);
        } else {
          module.exports.users.getUserById(user.id, done);
        }
      } else {
        done(err, user);
      }
    });
  });

  passport.use(new BearerStrategy(function (accessToken, done) {
      //console.log('Bearer Strategy with token ' + accessToken);
      console.log('BEARER Strategy');
      module.exports.users.getUserByAccessToken(accessToken, function (err, user) {
        if (err) {
          return done(err);
        }
        if (!user) {
          return done(null, false);
        }

        let now = moment();
        let expired_after = moment(user.expiredAfter);
        if (expired_after.isValid() === false || now.isAfter(expired_after)) {
          console.log("Access token expired: " + accessToken);
          module.exports.users.removeAccessToken(accessToken, function (err) {
            if (err) {
              return done(err);
            } else {
              return done(null, false, {message: 'access_token expired'});
            }
          });
        } else {
          const info = {scope: '*'};

          done(null, {id: user.idUser, username: user.username, initials: user.initials, client: user.client}, info);
        }
      });
    }
  ));

  passport.use(new BasicStrategy({realm: 'Finanzkraft'}, function (username, password, done) {
      console.log("BASIC STRATEGY", username);
      findByUsername(username, function (err, user) {
        const errorMessage = "Email oder Passwort ist falsch.";

        if (err) {
          console.log("ERROR from findByUsername", err);
          return done(err);
        }
        if (!user) {
          return done(null, false, {message: errorMessage});
        }
        if (module.exports.users.checkPassword(user, password)) {
          delete user.passwordSalt;
          delete user.passwordHash;
          console.log("password was ok", user);
          done(null, user);
        } else {
          console.log("Password was wrong");
          return done(null, false, {message: errorMessage});
        }
      });
    }
  ));

  let findByUsername = function (username, callback) {
    let options = {};
    module.exports.users.getUserByUsername(username, {}, callback);
  };

  let findUser = function (userSpec, done) {
    if (userSpec.isNotLocalUser) {
      const provider = userSpec.provider;
      const providerKey = userSpec.providerKey;
      findByProviderKey(provider, providerKey, function (err, user) {
        if (err) {
          console.log('Error in findUser. Error returned from findByProviderKey: ' + err);
          done(err);
        }
        else {
          if (user) {
            done(null, user);
          }
          else {
            const id = userSpec.providerAndKey;
            if (id && notRegisteredUsers[id]) {
              console.log('findUser: found not registered user with provider key ' + id);
              done(null, notRegisteredUsers[id]);
            } else {
              console.log('findUser: found unknown user with provider key ' + id);
              done(null, false);
            }
          }
        }
      });
    } else {
      module.exports.users.getUserById(userSpec.id, done);
    }
  };

  let setAccessTokenForUser = function (userId, tokenData, callback) {
    if (userId === undefined) {
      let error = new Error("Attempt to setAccessTokenForUser with undefined userId");
      error.status = 400; // Bad Request
      callback(error);
      return;
    }
    if (!tokenData) {
      let error = new Error("Attempt to setAccessTokenForUser with undefined tokenData");
      error.status = 400; // Bad Request
      callback(error);
      return;
    }
    if (!tokenData.client_id) {
      let error = new Error("Attempt to set setAccessTokenForUser with undefined client_id");
      error.status = 400; // Bad Request
      callback(error);
      return;
    }
    module.exports.users.setAccessToken(userId, tokenData, callback);
  };

  let createAccessTokenForUser = function (userId, client_id, callback) {
    if (userId === undefined) {
      let error = new Error("ERROR: attempt to createAccessTokenForUser with undefined userId");
      error.status = 400; // Bad Request
      callback(error);
      return;
    }
    if (!client_id) {
      let error = new Error("ERROR: attempt to set createAccessTokenForUser with undefined client_id");
      error.status = 400; // Bad Request
      callback(error);
      return;
    }
    findUser({id: userId}, function () {
      const tokenValue = hat().toString('base64');
      const refreshTokenValue = hat().toString('base64');
      const expires = config.get('tokenLifetime');
      const tokenData = {
        client_id: client_id,
        access_token: tokenValue,
        expires_in: expires,
        refresh_token: refreshTokenValue
      };
      setAccessTokenForUser(userId, tokenData, function (err) {
        if (_.isFunction(callback)) {
          if (err) {
            callback(err);
          } else {
            callback(null, tokenData);
          }
        }
      });
    });
  };

  let refreshTokenForUser = function (data, callback) {
    let error;
    console.log('Find user for refresh_token');
    module.exports.users.getUserByRefreshToken(data.refresh_token, function (err, user) {
      if (err) {
        let description;
        if (err instanceof Error) {
          description = err.message;
        } else {
          description = err;
        }
        error = new Error(description);
        error.status = 500; // Internal Server Error
        callback(error);
        return;
      }
      if (!user) {
        error = new Error('No user found for refresh token');
        error.status = 400; // Bad Request
        callback(error);
        return;
      }

      const accessTokenSpec = user.accessTokens[data.client_id];

      console.log('Found ' + user.username + ' for given refresh_token');

      createAccessTokenForUser(user.id, data.client_id, function (err, tokenData) {
        if (err) {
          let description;
          if (err instanceof Error) {
            description = err.message;
          } else {
            description = err;
          }
          error = new Error(description);
          error.status = 500; // Internal Server Error
          callback(error);
          return;
        }
        callback(null, tokenData);
      })
    });

  };

  module.exports.findByUsername = findByUsername;
  module.exports.findUser = findUser;
  module.exports.setAccessTokenForUser = setAccessTokenForUser;
  module.exports.createAccessTokenForUser = createAccessTokenForUser;
  module.exports.refreshTokenForUser = refreshTokenForUser;

  if (_.isFunction(callback)) {
    callback(null);
  }
};

// Simple route middleware to ensure user is authenticated.
//   Use this route middleware on any resource that needs to be protected.  If
//   the request is authenticated (typically via a persistent login session),
//   the request will proceed.  Otherwise, the user will be redirected to the
//   login page.
let ensureAuthenticated = function (req, res, next) {
  if (req.isAuthenticated()) {
    return next();
  }
  const nextUrl = req.route.path;
  if (nextUrl && nextUrl != '/') {
    res.redirect('/login?nexturl=' + nextUrl);
  } else {
    res.redirect('/login');
  }
  return null;
};

module.exports.ensureAuthenticated = ensureAuthenticated;

module.exports.ensureAuthenticatedForApi = function (req, res, next) {
  if (req.isAuthenticated()) {
    return next();
  }
  res.statusCode = 401;
  res.send('401 Unauthorized');
  return null;
};

function findByProviderKey(providerName, providerKey, callback) {
  let self = this;
  fs.exists(this.usersFilename, function (exists) {
    if (exists) {
      jf.readFile(self.usersFilename, function (error, data) {
        if (error) {
          callback(error);
        } else {
          const userLogin = _.findWhere(data.userLogin, {loginProvider: providerName, providerKey: providerKey});
          if (userLogin) {
            module.exports.users.getUserById(userLogin.userId, callback);
          } else {
            callback("Login with providerName " + providerName + " and providerKey " + providerKey + " does not" +
              " exist");
          }
        }
      });
    } else {
      // file does not exist -> return error
      callback("No Users are stored.");
    }
  });
}
