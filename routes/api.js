const express = require('express');
const passport = require('passport');
const router = express.Router();
const _ = require('lodash');
const moment = require('moment');
const CORS = require('cors');
const DB = require('../database');
const config = require('../config');
const Users = require('../Users');
const Hat = require('hat');
const ibantools = require('ibantools');

// ### Security Konzept ###
// client hat private und public key
// server hat public key in DB und private key im RAM
// client sendet Anfrage mit deviceId signiert mittels private key
// server prüft deviceId mittels public key => deviceId ok, wenn signature ok
// server benutzt private key von deviceId zum entschlüsseln der Konto-Login Daten

/* POST: add/register a device */
router.post('/devices', CORS(), function (req, res, next) {
  if (!_.isObject(req.body)) {
    res.status(404).send('request body must be an object');
    return;
  }
  if (!req.body.deviceid) {
    res.status(404).send('request body must be an object with attribute deviceid');
    return;
  }
  if (!req.body.pubkey) {
    res.status(404).send('request body must be an object with attribute pubkey');
    return;
  }
  if (!req.body.privkey) {
    res.status(404).send('request body must be an object with attribute privkey');
    return;
  }
  // todo limit registrations from unwanted attackers that spam database
  if (req.body.deviceid && req.body.pubkey && req.body.privkey) {
    new Promise(async (resolve, reject) => {
      try {
        const database = new DB();
        const now = moment();
        const savedDeviceId = await database.addDevice(req.body.deviceid, req.body.pubkey, now);
        // private key is stored only in memory
        let devices = req.app.get('knownDevices');
        if (!devices) {
          devices = [];
          req.app.set('knownDevices', devices);
        }
        devices[savedDeviceId] = {privkey: req.body.privkey, registration: now};
        resolve(savedDeviceId);
      } catch (ex) {
        console.log("ERROR", ex);
        reject(ex);
      }
    }).then(savedDeviceId => {
      res.json({deviceId: savedDeviceId});
    }).catch(reason => {
      res.status(500).send('Adding device in database failed');
    });
  } else {
    res.status(404).send('request body must have correct parameters');
  }
});

/* POST: setup a new account for a device */
router.post('/accounts', CORS(), function (req, res, next) {
  if (!_.isObject(req.body)) {
    res.status(404).send('request body must be an object');
    return;
  }
  if (!req.body.deviceid) {
    res.status(404).send('request body must be an object with attribute deviceid');
    return;
  }
  if (!req.body.signature) {
    res.status(404).send('request body must be an object with attribute signature');
    return;
  }
  if (!req.body.iban) {
    res.status(404).send('request body must be an object with attribute iban');
    return;
  }
  if (!ibantools.isValidIBAN(req.body.iban)) {
    res.status(404).send('iban is invalid');
    return;
  }
  if (!req.body.bankcontact) {
    res.status(404).send('request body must be an object with attribute bankcontact');
    return;
  }
  if (!ibantools.isValidBIC(req.body.bankcontact.bic)) {
    res.status(404).send('bic in bankcontact is invalid');
    return;
  }
  console.log("adding a new account...");
  new Promise(async (resolve, reject) => {
    try {
      const deviceId = req.body.deviceid;
      const database = new DB();
      const deviceInfo = await database.getDeviceData(deviceId);

      // const devices = req.app.get('knownDevices');
      // const deviceInfo = devices[deviceId];
      // if (!deviceInfo) {
      //   // need to request an device info update from client, which then sends again the private key
      //   // to be stored in req.app.knownDevices
      //   console.log(`Device info not in memory for deviceId ${deviceId}. Client needs to resend the private key.`);
      //   resolve({ok: false, reason: 'privkey'});
      //   return;
      // }
      const data = Buffer.from(deviceId);
      const isVerified = crypto.verify("SHA256", data, deviceInfo.publicKey, req.body.signature);
      if (!isVerified) {
        // need to request an device info update from client, which then sends again the private key
        // to be stored in req.app.knownDevices
        console.log(`Signature verification for deviceId ${deviceId} failed`);
        resolve({ok: false, reason: 'signature'});
        return;
      }

      let bankContact = {
        type: req.body.bankcontact.type,
        bic: req.body.bankcontact.bic,
        url: req.body.bankcontact.url,
        username_enc: req.body.bankcontact.username_enc,
        password_enc: req.body.bankcontact.password_enc,
      };

      // note: user and password are not decrypted when stored in database
      const savedAccountId = await database.addAccount(req.body.deviceid, req.body.iban, bankContact, moment());
      resolve({ok: true, savedAccountId: savedAccountId});
    } catch (ex) {
      console.log("ERROR", ex);
      reject(ex);
    }
  }).then(result => {
    if (result.ok) {
      res.json({accountId: result.savedAccountId});
    } else {
      switch (result.reason) {
        default:
          res.status(401).end();  // unauthorized
      }
    }
  }).catch(reason => {
    res.status(500).send('Adding account in database failed');
  });
});

// ### alter code ###
// todo: lösche nicht mehr benutzten Code

function authenticate(req, res, next) {
  // check for bearer authentication header with token
  let token = '';
  if (req.headers && req.headers.authorization) {
    let parts = req.headers.authorization.split(' ');
    if (parts.length === 2) {
      let scheme = parts[0], credentials = parts[1];
      if (/^Bearer/i.test(scheme)) {
        token = credentials;
      }
    }
  }
  if (token) {
    passport.authenticate('bearer', {session: false})(req, res, next);
  } else {
    passport.authenticate('basic', {session: false})(req, res, next);
  }

}

/* POST create new access token for user (must be authenticate with basic auth) */
router.options('/auth', CORS()); // enable pre-flight
router.post('/auth', CORS(), authenticate, function (req, res, next) {
  let authUser = req.user.username;
  if (!authUser) {
    throw new Error("req.user.username must be defined");
  }
  console.log("AUTH " + authUser);

  let users = new Users();
  var tokenValue = Hat().toString('base64');
  var refreshToken = Hat().toString('base64');
  var expires = config.get('tokenLifetime');
  var tokenData = {
    client_id: authUser,
    access_token: tokenValue,
    expires_in: expires,
    refresh_token: refreshToken
  };

  users.setAccessToken(req.user.id, tokenData, function (err) {
    if (err) {
      console.log("can't set accessToken for user with id req.user.userId: " + req.user.id);
      res.status(500).send('Setting accessToken for user failed');
    } else {
      res.json({tokenData: tokenData});
    }
  });

});

// Create new user
router.post('/users', CORS(), function (req, res, next) {
  if (req.body && req.body.username && req.body.password) {
    users.createUser(req.username, req.password, function (err, createdUser) {
      if (err) {
        console.log("ERROR:", err);
        res.status(500);
        res.end();
      } else {
        res.json(
          {
            id: createdUser.id,
            username: createdUser.username,
            initials: createdUser.initials,
            roleIds: createdUser.roleIds
          });
      }
    });
  } else {
    console.log('Bad request: username and/or password missing in request body');
    res.status(400).end();
  }
});

/* GET transaction listing for multiple accounts. */
router.options('/transactions', CORS()); // enable pre-flight
router.get('/transactions', CORS(), authenticate, function (req, res, next) {
  let limit = 20;
  if (req.query.limit) {
    limit = parseInt(req.query.limit);
    if (_.isNaN(limit)) {
      limit = 20;
    } else {
      if (limit > 500) {
        limit = 500;
      }
    }
  }
  let accounts;
  let database = new DB();
  database.getTransactions(accounts, limit, function (err, transactions) {
    if (err) {
      console.log("ERROR in getTransactions:", err);
      res.status(500).send('Accessing transactions in database failed');
    } else {
      res.json(transactions);
    }
  });
});

/* GET transaction listing. */
router.get('/accounts/:accountid/transactions', CORS(), authenticate, function (req, res, next) {
  const accountId = req.params.accountid;
  let limit = 10; // todo parse query string for limit
  if (accountId >= 0) {
    let database = new DB();
    database.getTransactions(accountId, limit, function (err, transactions) {
      if (err) {
        res.status(500).send('Accessing transactions in database failed');
      } else {
        res.json(transactions);
      }
    });
  } else {
    res.status(404).send('no account number specified');
  }
});

/* POST (add) transactions */
router.post('/accounts/:accountid/transactions', CORS(), authenticate, function (req, res, next) {
  const accountId = req.params.accountid;
  let authUser = req.user.username;
  if (!authUser) {
    throw new Error("req.user.username must be defined");
  }
  if (!_.isObject(req.body)) {
    res.status(404).send('request body must be an object');
    return;
  }
  if (!req.body.transactions) {
    res.status(404).send('request body must be an object with attribute transactions');
    return;
  }
  if (!req.body.balance) {
    res.status(404).send('request body must be an object with attribute balance');
    return;
  }
  if (!_.isObject(req.body.balance) || req.body.balance.balance === undefined || !req.body.balance.balanceDate) {
    res.status(404).send('request body.balance must be an object with attribute balance and balanceDate');
    return;
  }
  if (_.isArray(req.body.transactions)) {
    if (accountId >= 0) {
      let database = new DB();
      database.addTransactions(authUser, accountId, req.body.transactions, req.body.balance, function (err, savedTransactions) {
        if (err) {
          console.log("ERROR", err);
          res.status(500).send('Adding transactions in database failed');
        } else {
          res.json(savedTransactions);
        }
      });
    } else {
      res.status(404).send('no account number specified');
    }
  } else {
    res.status(404).send('request body must be array of transactions');
  }
});

/* GET accounts listing. */
router.get('/accounts', CORS(), authenticate, function (req, res, next) {
  let database = new DB();
  database.getAccounts(function (err, accounts) {
    if (err) {
      res.status(500).send('Accessing accounts in database failed');
    } else {
      res.json(accounts);
    }
  });
});

/* POST (add) transactions to the users cash account */
router.post('/CashTransactions', passport.authenticate('basic', {session: false}), function (req, res, next) {
  console.log("CashTransactions:", req.user);
  let authUser = req.user.username;
  if (!authUser) {
    console.log("No authUser");
    throw new Error("req.user.username must be defined");
  }
  console.log("BODY:", req.body);
  let transactions;

  if (_.isArray(req.body)) {
    let invalid = _.find(req.body, function (t) {
      let tInvalid = t.Amount === undefined || t.Payee === undefined || t.Text === undefined || t.TransactionDate ===
        undefined;
      if (tInvalid) {
        console.log("The following entry in the body array is invalid:", t);
      }
      return tInvalid;
    });
    if (invalid) {
      res.status(400).send('transaction data does not have the expected keys');
      return;
    }

    transactions = _.map(req.body, function (t) {
      let transaction = {
        modifiedBy: authUser,
        amount: t.Amount,
        payeePayerName: t.Payee,
        paymentPurpose: t.Text,
        valueDate: t.TransactionDate,
        category: t.Category,
        coordinates: {longitude: t.Coordinates.Longitude, latitude: t.Coordinates.Latitude}
      };
      return transaction;
    });
  } else {
    /*
        { Category: 'Essen:verzichtbar',
            Payee: 'Bäcker',
            Amount: -1.95,
            Coordinates: { Longitude: 11.76537296735846, Latitude: 48.104399275079466 },
          TransactionDate: '2018-07-19 07:18:55 +0200',
              Text: '' }
    */

    if (req.body.Amount === undefined || req.body.Payee === undefined || req.body.Text === undefined || req.body.TransactionDate === undefined) {
      res.status(400).send('transaction data does not have the expected keys');
      return;
    } else {
      let transaction = {
        modifiedBy: authUser,
        amount: req.body.Amount,
        payeePayerName: req.body.Payee,
        paymentPurpose: req.body.Text,
        valueDate: req.body.TransactionDate,
        category: req.body.Category,
        coordinates: {longitude: req.body.Coordinates.Longitude, latitude: req.body.Coordinates.Latitude}
      };

      transactions = [transaction];
    }
  }
  let database = new DB();
  database.addTransactionsToUsersCashAccount(req.user.username, transactions, function (err, savedTransactions) {
    if (err) {
      console.log("ERROR", err);
      res.status(500).send('Adding transactions in database failed');
    } else {
      res.json(savedTransactions);
    }
  });
});

router.get('/CashTransactions', passport.authenticate('basic', {session: false}), function (req, res, next) {
  console.log("GET CashTransactions:", req.user);
  res.json({ok: true});
});

module.exports = router;
