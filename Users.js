const _ = require('lodash');
const crypto = require('crypto');
const moment = require('moment');
const config = require('./config');

let Users = module.exports = function () {
  this.initialize.apply(this, arguments);
};

_.extend(Users.prototype, {
  initialize: function () {
    const dbhost = config.get('dbhost');
    const dbname = config.get('dbname');
    const dbuser = config.get('dbuser');
    const dbuserpass = config.get('dbuserpass');
    const dbdebug = config.get('dbdebug');

    this.knex = require('knex')({
      client: 'mssql',
      connection: {
        host: dbhost,
        user: dbuser,
        password: dbuserpass,
        database: dbname,
        options: {
          encrypt: true
        }
      },
      debug: dbdebug,
      pool: {
        min: 0, max: 30
      }
    });
  },

  createUser: function (username, password, callback) {
    let self = this;
    this.getUserByUsername(username, {}, function (err, existingUser) {
      if (err) {
        callback(err);
      } else {
        if (existingUser) {
          callback("Can't create user " + username + ", because it already exists");
        } else {
          // ok - user does not already exist
          let salt = self._createSalt();
          let passwordHash = self._encryptPassword(password, salt);
          self.addUser(username, salt, passwordHash, function (err, user) {
            if (user) {
              delete user.passwordSalt;
              delete user.passwordHash;
            }
            callback(err, user);
          });
        }
      }
    });
  },

  checkPassword: function (user, password) {
    if (user) {
      if (!user.passwordHash) {
        return false;
      }
      if (!user.passwordSalt) {
        return false;
      }
      return this._encryptPassword(password, user.passwordSalt) === user.passwordHash;
    } else {
      return false;
    }
  },

  getUserByUsername: function (username, options, callback) {
    if (!username) {
      callback("username must not be undefined");
      return;
    }

    options || (options = {});

    this.knex.select().table('Users')
    .where(function () {
      this.where('username', username)
    })
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        callback(null, queryResult[0]);
      } else {
        console.log("User with username " + username + " does not exist.");
        callback();
      }
    })
    .catch(function (err) {
      console.log('Failed to select Users', err);
      callback(err);
    });
  },

  getUserByAccessToken: function (accessToken, callback) {
    if (!accessToken) {
      callback("Can't get user by undefined access token.");
      return;
    }
    if (!_.isString(accessToken)) {
      callback("Can't get user by non-string access token.");
      return;
    }

    this.knex.select().table('Users')
    .join('UsersAccessTokens', function () {
      this.on('Users.id', '=', 'UsersAccessTokens.idUser')
    })
    .where(function () {
      this.where('accessToken', accessToken)
    })
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        callback(null, queryResult[0]);
      } else {
        console.log("User with access token ", accessToken, " does not exist.");
        callback();
      }
    })
    .catch(function (err) {
      console.log('Failed to select User by access token', err);
      callback(err);
    });
  },

  getUserByRefreshToken: function (refreshToken, callback) {
    if (!refreshToken) {
      callback("Can't get user by undefined refresh token.");
      return;
    }
    if (!_.isString(refreshToken)) {
      callback("Can't get user by non-string refresh token.");
      return;
    }

    this.knex.select().table('Users')
    .join('UsersAccessTokens', function () {
      this.on('Users.id', '=', 'UsersAccessTokens.idUser')
    })
    .where(function () {
      this.where('refreshToken', refreshToken)
    })
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        callback(null, queryResult[0]);
      } else {
        console.log("User with refresh token ", refreshToken, " does not exist.");
        callback();
      }
    })
    .catch(function (err) {
      console.log('Failed to select User by refresh token', err);
      callback(err);
    });
  },

  removeAccessToken: function (accessToken, callback) {
    if (!accessToken) {
      callback("Can't remove undefined access token.");
      return;
    }
    if (!_.isString(accessToken)) {
      callback("Can't remove non-string access token.");
      return;
    }

    knex('UsersAccessTokens')
    .where('accessToken', accessToken)
    .del()
    .then(function () {
      console.log("Access token " + accessToken + " does not exist and can't be deleted.");
      callback();
    })
    .catch(function (err) {
      console.log('Failed to delete access token ' + accessToken, err);
      callback(err);
    });
  },

  getUserById: function (userId, callback) {
    if (!accessToken) {
      callback("Can't select user by undefined userId.");
      return;
    }
    if (!_.isNumber(accessToken)) {
      callback("Can't select user by non-number userId.");
      return;
    }
    this.knex.select().table('Users')
    .where(function () {
      this.where('id', userId)
    })
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        callback(null, queryResult[0]);
      } else {
        console.log("User with id ", userId, " does not exist.");
        callback();
      }
    })
    .catch(function (err) {
      console.log('Failed to select Users', err);
      callback(err);
    });
  },

  getForIds: function (userIds, callback) {
    if (!userIds) {
      callback("Can't get user by undefined userIds.");
      return;
    }
    if (!_.isArray(userIds)) {
      callback("Can't get user by non-array userIds.");
      return;
    }
    this.knex.select().table('Users')
    .whereIn('id', userIds)
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        callback(null, queryResult);
      } else {
        callback(null, []);
      }
    })
    .catch(function (err) {
      console.log('Failed to select Users by userIds', err);
      callback(err);
    });
  },

  addUser: function (username, passwordSalt, passwordHash, callback) {
    if (!username) {
      callback("Not adding user with undefined username");
      return;
    }

    this.knex('Users').insert({username: username, passwordSalt: passwordSalt, passwordHash: passwordHash, initials: username.substr(0, 2).toLocaleUpperCase()})
    .returning('id')
    .then(function (insertResult) {
      if (_.isArray(insertResult) && insertResult.length > 0) {
        let id = insertResult[0];
        console.log("User " + username + " added with id " + id);
        callback(null, id);
      } else {
        callback("User " + username + " was not added to the Users table in the database.");
      }
    })
    .catch(function (err) {
      console.log('Failed to insert user ' + username);
      callback(err);
    });
  },

  /* updates user information - without password and password hash */
  saveUser: function (user, callback) {
    if (!user.id || !user.username) {
      const err = "ERROR: attempt to save incomplete user";
      console.log(err);
      callback(err);
      return;
    }

    this.knex('Users').where('id', user.id)
    .update({
      initials: user.initials,
      username: user.username,
      passwordSalt: user.passwordSalt,
      passwordHash: user.passwordHash
    })
    .then(function (updateResult) {
      console.log("USER UPDATED:", updateResult);
      callback();
    })
    .catch(function (err) {
      console.log('Failed to update user', err);
      callback(err);
    });
  },

  deleteUser: function (userId, callback) {
    if (!userId) {
      let err = "ERROR: attempt to delete user with undefined userId";
      console.log(err);
      callback(err);
      return;
    }
    knex('Users')
    .where('id', userId)
    .del()
    .then(function () {
      console.log("User with id " + userId + " does not exist and can't be deleted.");
      callback();
    })
    .catch(function (err) {
      console.log('Failed to delete user with id ' + userId, err);
      callback(err);
    });
  },

  setAccessToken: function (userId, tokenData, callback) {
    if (userId === undefined) {
      callback("ERROR: attempt to setAccessToken with undefined userId");
      return;
    }
    if (!tokenData) {
      callback("ERROR: attempt to setAccessToken with undefined tokenData");
      return;
    }
    if (!tokenData.client_id) {
      callback("ERROR: attempt to set setAccessToken with undefined client_id");
      return;
    }
    let knex = this.knex;
    knex('UsersAccessTokens')
    .where({'idUser': userId, client: tokenData.client_id})
    .then(function (queryResult) {
      let expiresAbsolute = new Date((new Date().getTime()) + (tokenData.expires_in - 5) * 1000).getTime();
      let expiredAfter = moment(expiresAbsolute);
      if (queryResult.length > 0) {
        // update
        knex('UsersAccessTokens').where({'idUser': userId, client: tokenData.client_id})
        .update({
          accessToken: tokenData.access_token,
          expiresIn: tokenData.expires_in,
          expiredAfter: expiredAfter.toDate(),
          refreshToken: tokenData.refresh_token
        })
        .then(function (updateResult) {
          console.log("UsersAccessTokens for " + userId + "/" + tokenData.client_id + " updated", updateResult);
          callback();
        })
        .catch(function (err) {
          console.log("Failed to update UsersAccessTokens for " + userId + "/" + tokenData.client_id, err);
          callback(err);
        });
      } else {
        // insert
        knex('UsersAccessTokens')
        .insert({
          idUser: userId,
          client: tokenData.client_id,
          accessToken: tokenData.access_token,
          expiresIn: tokenData.expires_in,
          expiredAfter: expiredAfter.toDate(),
          refreshToken: tokenData.refresh_token
        })
        .then(function () {
          console.log("New item inserted into UsersAccessTokens for " + userId + "/" + tokenData.client_id);
          callback();
        })
        .catch(function (err) {
          console.log("Failed to insert new item into UsersAccessTokens for " + userId + "/" + tokenData.client_id, err);
          callback(err);
        });
      }
    })
    .catch(function (err) {
      console.log('Failed to select accessToken for userId ' + userId, err);
      callback(err);
    });

  },

  _createSalt: function () {
    const salt = crypto.randomBytes(32).toString('base64');
    return salt;
  },

  _encryptPassword: function (password, salt) {
    return crypto.createHmac('sha1', salt).update(password).digest('hex');
    //more secure – return crypto.pbkdf2Sync(password, this.salt, 10000, 512);
  }

});

// Helpers
// -------

// Helper function to correctly set up the prototype chain, for subclasses.
// Similar to `goog.inherits`, but uses a hash of prototype properties and
// class properties to be extended.
let extend = function (protoProps, staticProps) {
  let parent = this;
  let child;

  // The constructor function for the new subclass is either defined by you
  // (the "constructor" property in your `extend` definition), or defaulted
  // by us to simply call the parent's constructor.
  if (protoProps && _.has(protoProps, 'constructor')) {
    child = protoProps.constructor;
  } else {
    child = function () {
      return parent.apply(this, arguments);
    };
  }

  // Add static properties to the constructor function, if supplied.
  _.extend(child, parent, staticProps);

  // Set the prototype chain to inherit from `parent`, without calling
  // `parent`'s constructor function.
  let Surrogate = function () {
    this.constructor = child;
  };
  Surrogate.prototype = parent.prototype;
  child.prototype = new Surrogate();

  // Add prototype properties (instance properties) to the subclass,
  // if supplied.
  if (protoProps) {
    _.extend(child.prototype, protoProps);
  }

  // Set a convenience property in case the parent's prototype is needed
  // later.
  child.__super__ = parent.prototype;

  return child;
};

Users.extend = extend;
