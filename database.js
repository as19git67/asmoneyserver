const _ = require('lodash');
const config = require('./config');
const moment = require('moment');
const crypto = require('crypto');


class DB {
  constructor(options) {
    options || (options = {});

    this.KATEGORIE_VERWENDUNG = {NO_CATEGORY: 0};

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
          encrypt: true,
          trustServerCertificate: true,
          requestTimeout: 30000
        }
      },
      debug: dbdebug,
      pool: {
        min: 0, max: 30
      }
    });
  }

  isSchemaOK(callback) {
    const table = 'Users';
    this._existsTable(table, callback);
  }

  makeSchemaUpToDate(callback) {
    this._createTables(callback);
  }

  _createTables(cb) {
    let self = this;
    async.waterfall([
        function Start(callback) {
          console.log('Creating database tables...');
          callback();
        },
        function (callback) {
          self._dropall(['UsersAccessTokens', 'Users', 'NewTransactions', 'NewAccounts', 'FinTsContacts'], callback);
        },

        // CREATE TABLES
        function (callback) {
          const tableName = 'Users';
          self.knex.schema.createTable(tableName, function (t) {
            t.increments('id').primary();
            t.string('username').unique().notNullable().index();
            t.string('passwordSalt').notNullable();
            t.string('passwordHash').notNullable();
            t.string('initials', 2);
          })
          .then(function () {
            console.log("Table " + tableName + " created");
            callback();
          })
          .catch(function (err) {
            console.log("creating table " + tableName + " failed");
            callback(err);
          });
        },

        function (callback) {
          const tableName = 'UsersAccessTokens';
          self.knex.schema.createTable(tableName, function (t) {
            t.increments('id').primary();
            t.integer('idUser').notNullable().references('id').inTable('Users').index();
            t.string('client').notNullable().index();
            t.string('accessToken').notNullable().index();
            t.string('refreshToken').notNullable().index();
            t.integer('expiresIn').notNullable();
            t.dateTime('expiredAfter').notNullable().index();
          })
          .then(function () {
            console.log("Table " + tableName + " created");
            callback();
          })
          .catch(function (err) {
            console.log("creating table " + tableName + " failed");
            callback(err);
          });
        },

        function (callback) {
          const tableName = 'FinTsContacts';
          self.knex.schema.createTable(tableName, function (t) {
            t.increments('id').primary();
            t.string('Name').unique().notNullable();
          })
          .then(function () {
            console.log("Table " + tableName + " created");
            callback();
          })
          .catch(function (err) {
            console.log("creating table " + tableName + " failed");
            callback(err);
          });
        },
        function (callback) {
          self._switchSystemVersioningOn('FinTsContacts', callback);
        },

        function (callback) {
          const tableName = 'NewAccounts';
          self.knex.schema.createTable(tableName, function (t) {
            t.increments('id').primary();
            t.string('name').notNullable().index();
          })
          .then(function () {
            console.log("Table " + tableName + " created");
            callback();
          })
          .catch(function (err) {
            console.log("creating table " + tableName + " failed");
            callback(err);
          });
        },
        function (callback) {
          self._switchSystemVersioningOn('NewAccounts', callback);
        },

        function (callback) {
          const tableName = 'NewTransactions';
          self.knex.schema.createTable(tableName, function (t) {
            t.increments('id').primary();

            t.string('OwnrAcctCcy').notNullable();
            t.string('OwnrAcctIBAN').notNullable();
            t.string('OwnrAcctNo').notNullable();
            t.string('OwnrAcctBIC').notNullable();
            t.string('OwnrAcctBankCode').notNullable();

            t.dateTime('bookingDate', false).index();   // = Buchungsdatum;  false means database does store timezone
            t.dateTime('valueDate', false).index();   // = Wertstellung = Valuta; with timezone
            t.integer('amount').notNullable();
            t.string('amountCurrency').notNullable();

            t.bool('isCreditor').notNullable(); // CdtDbtInd -> Creditor - Debitor
            t.string('EndToEndId').index();
            t.string('PmtInfId').index();
            t.string('MndtId').index();
            t.string('CdtrId').index();
            t.string('RmtInf');
            t.string('PurpCd').index();
            t.string('BookgTxt').notNullable().index();
            t.string('PrimaNotaNo');

            t.string('BankRef').index();
            t.string('BkTxCd').index();
            t.string('RmtdNm').index();
            t.string('RmtdAcctCtry').index();
            t.string('RmtdAcctIBAN').index();
            t.string('RmtdAcctNo').index();
            t.string('RmtdAcctBIC').index();
            t.string('RmtdAcctBankCode').index();
            t.string('Category').index();
            t.string('Notes');
            t.string('BookgSts').index();
            t.string('GVC').index();
            t.string('GVCExtension').index();
            t.string('BtchBookg').index();
            t.string('BtchId').index();
            t.string('RmtdUltmtNm').index();
            t.integer('idAccount').notNullable().references('id').inTable('NewAccounts').index();
            t.dateTime('transactionDate', false);   // = Buchungsdatum;  false means database does store timezone
          })
          .then(function () {
            console.log("Table " + tableName + " created");
            callback();
          })
          .catch(function (err) {
            console.log("creating table " + tableName + " failed");
            callback(err);
          });
        },
        function (callback) {
          self._switchSystemVersioningOn('NewTransactions', callback);
        }
      ],
      function Complete(err, result) {
        cb(err);
      }
    );
  }

  _existsTable(table, callback) {
    //    knex.raw("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME='" + table + "'").then(function (queryResult) {
    this.knex('INFORMATION_SCHEMA.TABLES').where({TABLE_NAME: table}).count('* as cnt').then(function (queryResult) {
      let cnt = queryResult[0].cnt;
      callback(null, cnt > 0);
    }).catch(function (err) {
      console.log('Query to check whether table ' + table + ' exists failed');
      callback(err);
    });
  }

  // Execute all functions in the array serially
  _switchSystemVersioningOff(table, callback) {
    this.knex.raw('ALTER TABLE dbo.' + table + ' SET (SYSTEM_VERSIONING = OFF)').then(function () {
      console.log('System versioning switched OFF for table ' + table);
      callback();
    }).catch(function (err) {
      if (err.number === 13591) {
        // ignore error when system versioning is not turned on
        callback();
      } else {
        console.log('Switching system versioning off failed for table ' + table);
        callback(err);
      }
    });
  }

  _switchSystemVersioningOn(table, callback) {
    this.knex.raw('ALTER TABLE dbo.' + table +
      ' ADD SysStartTime datetime2 GENERATED ALWAYS AS ROW START NOT NULL, SysEndTime datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (SysStartTime,SysEndTime)').then(
      function () {
        console.log('System versioning switched ON for table ' + table);
        callback();
      }).catch(function (err) {
      console.log('altering table ' + table + ' failed');
      callback(err);
    });
  }

  _dropall(tables, callback) {
    if (!_.isArray(tables)) {
      callback("tables argument must be an array with table names");
      return;
    }

    const self = this;
    async.eachSeries(tables, function (table, callback) {

      self._existsTable(table, function (err, exists) {
        if (err) {
          console.log("checking for table " + table + " failed");
          callback(err);
        } else {
          if (exists) {
            self.knex.raw('DROP TABLE dbo.' + table).then(function () {
              console.log('Table ' + table + ' dropped');
              callback();
            }).catch(function (err) {
              console.log('dropping table ' + table + ' failed');
              callback(err);
            });
          } else {
            console.log("Table " + table + " not dropping, because it does not exist.");
            callback();
          }
        }
      });

    }, function (err) {
      if (err) {
        // One of the iterations produced an error.
        // All processing will now stop.
        console.log('Failed to drop a table');
        callback(err)
      } else {
        console.log('All tables have been dropped successfully');
        callback();
      }
    });
  }

  getAccounts(callback) {
    //&& ((a.Typ.HasValue && a.Typ == 1) || a.Typ.HasValue == false) && now < a.
    this.knex.select().table('s_konten')
    .where(function () {
      this.where('deleted', false)
    })
    .andWhere(function () {
      this.where('geschlossen', '>', moment().toDate())
    })
    .then(function (queryResult) {
      let accounts = _.map(queryResult, function (a) {
        return {
          id: a.id_konto,
          idFinTsContact: a.id_bankkontakt,
          bankCode: a.BankCode,
          accountCode: a.Nummer ? a.Nummer.trim() : '',
          acctNo: a.Nummer ? a.Nummer.trim() : '',
          description: a.Bezeichnung ? a.Bezeichnung.trim() : '',
          owner: a.Inhaber ? a.Inhaber.trim() : '',
          currency: a.Waehrung,
          startAmount: a.Anfangsbestand,
          idAccountType: a.Typ,
          iban: a.IBAN ? a.IBAN.trim() : '',
          lastDownload: a.UmsaetzeGeholt,
          deleted: a.deleted,
          closedSince: a.geschlossen
        }
      });
      callback(null, accounts);
    }).catch(function (err) {
      console.log('failed to select accounts', err);
      callback(err);
    });
  }

  _mapTransactionsFromDB(transactionsFromDB) {
    let transactions = _.map(transactionsFromDB, function (t) {
      if (t.Name) {
        t.payeePayerName = t.Name.trim();
        delete t.Name;
      }
      return {
        amount: t.Betrag,
        valueDate: t.Datum,
        paymentPurpose: t.Verwendungszweck ? t.Verwendungszweck.trim() : '',
        entryText: t.buchungstext ? t.buchungstext.trim() : '',
        idAccountBalance: t.id_accountbalance,
        idOutboundBasket: t.id_bankausgangskorb,
        idCategory: t.id_category,
        idClass1: t.id_klasse1,
        idClass2: t.id_klasse2,
        idAccount: t.id_konto,
        idOriginalTransaction: t.id_originaltransaction,
        id: t.id_transaction,
        idPmntPayee: t.id_zahlungsempfaenger,
        payeePayerAcctNo: t.payeePayerAcctNo,
        payeePayerBankCode: t.payeePayerBankCode,
        payeePayerName: t.payeePayerName,
        md5hash: t.md5hash,
        modifiedDate: t.modified,
        modifiedBy: t.modifiedBy ? t.modifiedBy.trim() : '',
        type: t.type,
        valid_end: t.valid_end,
        valid_start: t.valid_start,
        zkaTranCode: t.zkaTranCode ? t.zkaTranCode.trim() : '',
        primaNotaNo: t.primaNotaNo,
        deleted: t.deleted
      }
    });
    return transactions;
  }

  getTransactions(accountId, limit, callback) {
    let self = this;
    if (_.isNaN(limit)) {
      limit = 500;
    } else {
      if (limit > 500) {
        limit = 500;
      }
    }

    let where = {'deleted': false};

    let whereIn;
    if (accountId !== undefined) {
      if (_.isArray(accountId)) {
        whereIn = {'id_konto': accountId}
      } else {
        where['id_konto'] = accountId;
      }
    }

    if (whereIn !== undefined) {
      this.knex.select().table('transaction')
      .where(where)
      .whereIn(whereIn)
      .whereNull('valid_end')
      .limit(limit)
      .orderBy('Datum', 'desc')
      .then(function (queryResult) {
        let transactions = self._mapTransactionsFromDB(queryResult);
        callback(null, transactions);
      }).catch(function (err) {
        console.log('failed to select transactions', err);
        callback(err);
      });
    } else {
      this.knex.select().table('transaction')
      .where(where)
      .whereNull('valid_end')
      .limit(limit)
      .orderBy('Datum', 'desc')
      .then(function (queryResult) {
        let transactions = self._mapTransactionsFromDB(queryResult);
        callback(null, transactions);
      }).catch(function (err) {
        console.log('failed to select transactions', err);
        callback(err);
      });
    }
  }

  _getNotUsedCategoryId(callback) {
    let self = this;
    if (this._idCategoryNotUsed !== undefined) {
      callback(null, this._idCategoryNotUsed);
      return;
    }
    const idCategoryNotUsed = this.KATEGORIE_VERWENDUNG.NO_CATEGORY;
    this.knex.select().table('s_kategorieverwendung')
    .where({
      'id_kategorieverwendung': idCategoryNotUsed
    })
    .whereNotNull('id_category')
    .then(function (queryResult) {
      if (_.isArray(queryResult) && queryResult.length > 0) {
        self._idCategoryNotUsed = queryResult[0].id_category;
        callback(null, self._idCategoryNotUsed);
      } else {
        callback(); // no id_category found: return as successful, but no id_category
      }
    })
    .catch(function (err) {
      console.log('failed to select s_kategorieverwendung', err);
      callback(err);
    });
  }

  _getCashAccountForUser(username, callback) {
    console.log("Selecting user_preferences for " + username);
    this.knex.select().table('user_preferences')
    .where({
      'Username': username
    })
    .then(function (queryResult) {
      console.log("Results of query", queryResult);
      if (_.isArray(queryResult) && queryResult.length > 0) {
        let idAccount = queryResult[0].pref_id_konto_for_cash;
        callback(null, idAccount);
      } else {
        callback(); // no pref found: return as successful, but no id
      }
    })
    .catch(function (err) {
      console.log('failed to select user_preferences', err);
      callback(err);
    });
  }

  _createOrGetPayeePayerId(payeePayerName, payeePayerAcctNo, payeePayerBankCode, callback) {
    let knex = this.knex;
    if (_.isString(payeePayerName)) {
      let name = payeePayerName.trim();

      if (_.isString(payeePayerAcctNo)) {
        payeePayerAcctNo = payeePayerAcctNo.trim();
      } else {
        payeePayerAcctNo = undefined;
      }
      if (_.isString(payeePayerBankCode)) {
        payeePayerBankCode = payeePayerBankCode.trim();
      } else {
        payeePayerBankCode = undefined;
      }

      if (payeePayerBankCode && payeePayerAcctNo) {
        name = name + " (" + payeePayerBankCode + "/" + payeePayerAcctNo + ")";
      } else {
        if (payeePayerBankCode) {
          name = name + " (" + payeePayerBankCode + ")";
        }
      }

      knex.select().table('zahlungsempfaenger')
      .where({
        'Name': name
      })
      .then(function (queryResult) {
        if (_.isArray(queryResult) && queryResult.length > 0) {
          let idPayeePayer = queryResult[0].id_zahlungsempfaenger;
          callback(null, idPayeePayer)
        } else {
          // insert as new
          knex('zahlungsempfaenger').insert(
            {
              Name: name,
              PayeePayerBankcode: payeePayerBankCode ? payeePayerBankCode : undefined,
              PayeePayerAccountnumber: payeePayerAcctNo ? payeePayerAcctNo : undefined
            })
          .returning('id_zahlungsempfaenger')
          .then(function (result) {
            let id_zahlungsempfaenger = result[0];
            callback(null, id_zahlungsempfaenger);
          })
          .catch(function (err) {
            console.log("ERROR: failed to insert into zahlungsempfaenger");
            callback(err);
          });
        }
      })
      .catch(function (err) {
        console.log('ERROR: failed to select zahlungsempfaenger');
        callback(err);
      });
    } else {
      callback(); // can't search for no-string payee payer name
    }
  }

  addTransactionsToUsersCashAccount(username, transactions, callback) {
    let self = this;
    this._getCashAccountForUser(username, function (err, accountId) {
      if (err) {
        callback(err);
      } else {
        if (accountId !== undefined) {
          self.addTransactions(username, accountId, transactions, null, callback);
        } else {
          callback("no cash account configured for user with id " + userId);
        }
      }
    });
  }

  _getCategories(callback) {
    let knex = this.knex;
    knex.select().table('s_kategorien')
    .where('level', 1)
    .then(function (levelOneCategories) {
      knex.select().table('s_kategorien')
      .where('level', 2)
      .then(function (levelTwoCategories) {

        let categoriesByName = {};
        let categoriesById = {};
        let levelOneCategoriesById = {};
        _.each(levelOneCategories, function (c) {
          levelOneCategoriesById[c.id_category] = c;
          categoriesById[c.id_category] = c;
          categoriesByName[c.Name.trim().toLowerCase()] = c.id_category;
        });

        _.each(levelTwoCategories, function (c) {
          let levelOneCategory = levelOneCategoriesById[c.id_parent_category];
          categoriesById[c.id_category] = c;
          categoriesByName[levelOneCategory.Name.trim().toLowerCase() + ':' + c.Name.trim().toLowerCase()] = c.id_category;
        });

        callback(null, categoriesByName, categoriesById);
      })
      .catch(function (err) {
        console.log("ERROR selecting level 2 categories in database");
        callback(err);
      });

    })
    .catch(function (err) {
      console.log("ERROR selecting level 1 categories in database");
      callback(err);
    });
  }

  _makeTransactionObject(amount, valueDateStr, paymentPurpose, payeePayerAcctNo, payeePayerBankCode,
                         payeePayerName, entryText, type, gvCode, primaNotaNo, categoryStr, modifiedBy,
                         accountId, idAccountBalance, idCategoryNotUsed, categoriesByName, categoriesById,
                         valid_end, bankTransactionId) {

    let now = moment().toDate();

    let md5hash;

    if (bankTransactionId === undefined) {
      let md5Input = valueDateStr +
        amount.toString() +
        (paymentPurpose ? paymentPurpose.toLowerCase() : '') +
        (payeePayerName ? payeePayerName.toLowerCase() : '') +
        (payeePayerAcctNo ? payeePayerAcctNo : '') +
        (payeePayerBankCode ? payeePayerBankCode : '') +
        (entryText ? entryText.toLowerCase() : '') +
        accountId +
        (primaNotaNo ? primaNotaNo : '') +
        (gvCode ? gvCode : '');

      md5hash = crypto.createHash('md5').update(md5Input).digest('hex');
    } else {
      md5hash = bankTransactionId;
    }

    // determine the "oldest" transaction date
    let valueDate = moment(valueDateStr); // possible: 2018-07-19 16:57:51 +0200 -> 2018-07-19T16:57:51+0200
    if (!valueDate.isValid()) {
      throw new Error("valueDate can't be parsed by moment.js");
    }

    let idCategory = idCategoryNotUsed;
    if (categoryStr) {
      if (typeof categoryStr === "string") {
        const parsed = parseInt(categoryStr);
        if (isNaN(parsed) || parsed.toString() !== categoryStr.trim()) {
          let idC = categoriesByName[categoryStr.trim().toLowerCase()];
          if (idC !== undefined) {
            idCategory = idC;
          }
        } else {
          if (parsed !== 0) {
            idCategory = parsed;
          }
        }
      } else {
        idCategory = categoryStr; // not a string - must be a number
      }
    }
    if (!categoriesById[idCategory]) {
      console.log("Warning: non existing category id specified: " + idCategory + ". Not using it.")
      idCategory = idCategoryNotUsed;
    }

    return {
      id_originaltransaction: 0,  // if null or 0 it will be replaced by id_transaction in trigger function
      id_konto: accountId,
      id_category: idCategory,
      id_accountbalance: idAccountBalance,
      Betrag: amount,
      Datum: valueDate.toDate(),
      Verwendungszweck: paymentPurpose,
      payeePayerAcctNo: payeePayerAcctNo,
      payeePayerBankCode: payeePayerBankCode,
      payeePayerName: payeePayerName,
      buchungstext: entryText,
      type: type,
      zkaTranCode: gvCode === undefined ? '000' : gvCode,
      primaNotaNo: primaNotaNo,
      md5hash: md5hash,
      modified: now,
      modifiedBy: modifiedBy,
      valid_end: valid_end,
      valid_start: now,
      bankTransactionId: bankTransactionId
    };
  }

  addTransactions(modifiedBy, accountId, transactions, balance, callback) {
    if (_.isNaN(accountId)) {
      callback("accountId must be a number");
      return;
    }
    let knex = this.knex;
    let self = this;
    this._getNotUsedCategoryId(function (err, idCategoryNotUsed) {
      if (err) {
        callback(err);
      } else {
        self._getCategories(function (err, categoriesByName, categoriesById) {
          if (err) {
            callback(err);
          } else {
            try {
              let oldestDate = moment();
              let hashes = [];
              let coordinatesByHash = {};
              let transactionsToInsert = _.map(transactions, function (t) {

                let tr = self._makeTransactionObject(t.amount, t.valueDate, t.paymentPurpose, t.payeePayerAcctNo, t.payeePayerBankCode,
                  t.payeePayerName, t.entryText, t.type, t.gvCode, t.primaNotaNo, t.category, modifiedBy, accountId, undefined,
                  idCategoryNotUsed, categoriesByName, categoriesById, null, t.bankTransactionId);

                // determine the "oldest" transaction date
                if (moment(tr.Datum).isBefore(oldestDate)) {
                  oldestDate = moment(tr.Datum);
                }

                if (t.coordinates) {
                  if (coordinatesByHash[tr.md5hash]) {
                    throw new Error("ERROR: md5hash is not unique");
                  }
                  coordinatesByHash[tr.md5hash] = t.coordinates;
                }

                return tr;
              });

              // quick fix (hack): when transactions are modified, the time part of  the Datum is set to 0 and to still
              // catch them when later selecting, we just start selecting from one whole day earlier
              oldestDate.subtract(1, 'day');

              // Select all transactions in the same timespan and use the md5hash values to
              // filter out transactions that are already stored in the database

              // select * from transaction where Datum >= oldestDate && id_konto = accountId

              knex.select().table('transaction')
              .where({
                'deleted': false,
                'id_konto': accountId
              })
              .where('Datum', '>=', oldestDate.toDate())
              .whereNull('valid_end')
              .whereNotNull('md5hash')
              .then(function (alreadySavedTransactions) {
                let transactionLookup = {};
                _.each(alreadySavedTransactions, function (t) {
                  transactionLookup[t.md5hash.trim()] = t;
                });

                console.log("Have " + transactionsToInsert.length + " before filtering out existing");

                let filteredTransactionsToInsert = _.filter(transactionsToInsert, function (t) {
                  let md5hash = t.md5hash.trim();
                  let tExists = transactionLookup[md5hash];
                  if (!tExists) {
                    hashes.push(md5hash);
                  }
                  return !tExists;
                });

                console.log("Have " + filteredTransactionsToInsert.length + " after filtering out existing");

                if (filteredTransactionsToInsert.length > 0) {

                  async.eachSeries(filteredTransactionsToInsert, function (t, callbackForEachTransaction) {
                    self._createOrGetPayeePayerId(t.payeePayerName, t.payeePayerAcctNo, t.payeePayerBankCode, function (err, idPayeePayer) {
                      if (err) {
                        callbackForEachTransaction(err);
                      } else {
                        t.id_zahlungsempfaenger = idPayeePayer;
                        delete t.payeePayerName;
                        delete t.payeePayerAcctNo;
                        delete t.payeePayerBankCode;
                        callbackForEachTransaction();
                      }
                    });
                  }, function (err) {
                    if (err) {
                      console.log(err);
                    } else {

                      knex.transaction(function (trx) {

                        console.log("inserting transactions...");

                        knex.batchInsert('transaction', filteredTransactionsToInsert)
                        .transacting(trx)
                        .then(function () {

                          console.log("Select again after inserting transactions. Hashes:", hashes);

                          knex.select().table('transaction')
                          .transacting(trx)
                          .leftJoin('zahlungsempfaenger', function () {
                            this.on('transaction.id_zahlungsempfaenger', '=', 'zahlungsempfaenger.id_zahlungsempfaenger')
                          })
                          .where({
                            'deleted': false,
                            'id_konto': accountId
                          })
                          .whereNull('valid_end')
                          .whereIn('md5hash', hashes)
                          .orderBy('Datum', 'desc')
                          .orderBy('id_transaction', 'desc')
                          .then(function (justSavedTransactions) {

                            console.log(justSavedTransactions.length + " transactions saved.");
                            let coordinatesToAdd = [];
                            _.each(justSavedTransactions, function (t) {
                              let md5hash = t.md5hash.trim();
                              let coordinates = coordinatesByHash[md5hash];
                              if (coordinates) {
                                coordinatesToAdd.push({
                                  id_transaction: t.id_transaction,
                                  latitude: coordinates.latitude,
                                  longitude: coordinates.longitude
                                })
                              } else {
                                console.log("Did not found transaction for coordinate. hash: " + md5hash + ".");
                              }
                            });

                            let transactionsResult = self._mapTransactionsFromDB(justSavedTransactions);

                            console.log("Coordinates to add: " + coordinatesToAdd.length);

                            knex.batchInsert('transaction_location', coordinatesToAdd)
                            .transacting(trx)
                            .then(function () {

                              console.log("Coordinates inserted into DB.");

                              // ### handle balance

                              if (!balance || balance.balanceDate === undefined || balance.balance === undefined) {
                                console.log("Have no balance to update. Committing DB transaction.");
                                trx.commit(transactionsResult);
                              } else {
                                let balanceDate = moment(balance.balanceDate);
                                // calculate balance by making the sum of all amounts in transactions for the account
                                knex.raw(
                                  "SELECT " +
                                  "(" +
                                  "SELECT Anfangsbestand from [s_konten] where id_konto = ?" +
                                  ") + (" +
                                  "SELECT SUM(Betrag) FROM [transaction] WHERE id_konto= ? AND deleted='false' AND valid_end" +
                                  " IS NULL AND Datum <= ?" +
                                  ") - (" +
                                  "SELECT ISNULL(SUM(Betrag), 0) FROM [transaction] WHERE id_konto_link = ? AND " +
                                  "deleted = 'false' AND valid_end IS NULL AND Datum <= ?" +
                                  ")"
                                  , [accountId, accountId, balanceDate.toDate(), accountId, balanceDate.toDate()])
                                //     .then(function (resp) { ... });
                                // knex('transaction').sum('Betrag')
                                //     .transacting(trx)
                                //     .where({
                                //       'deleted': false,
                                //       'id_konto': accountId
                                //     })
                                //     .where('Datum', '<=', balanceDate.toDate())
                                //     .whereNull('valid_end')
                                .transacting(trx)
                                .then(function (results) {
                                  let balanceAfterInsertingTransactions = _.values(results[0])[0];
                                  console.log("balanceAfterInsertingTransactions", balanceAfterInsertingTransactions);

                                  let balanceDateStr = balanceDate.format('L');
                                  let transactionAtBalanceDate = _.find(justSavedTransactions, function (t) {
                                    let d = moment(t.Datum).format('L');
                                    return d === balanceDateStr;
                                  });

                                  if (transactionAtBalanceDate) {
                                    if (transactionAtBalanceDate.id_accountbalance === undefined ||
                                      transactionAtBalanceDate.id_accountbalance === null) {
                                      knex('accountbalance').insert(
                                        {
                                          id_konto: accountId,
                                          Datum: balanceDate.toDate(),
                                          Betrag: balance.balance,
                                          downloaded: moment().toDate()
                                        })
                                      .transacting(trx)
                                      .returning('id_balance')
                                      .then(function (result) {
                                        let id_balance = result[0];

                                        if (balance.balance === balanceAfterInsertingTransactions) {
                                          let trIdForBalance = transactionAtBalanceDate.id_transaction;
                                          knex('transaction').update({id_accountbalance: id_balance})
                                          .where('id_transaction', trIdForBalance)
                                          .andWhere('deleted', false)
                                          .transacting(trx)
                                          .then(function (result) {
                                            console.log("Transaction with id " + trIdForBalance + " updated with" +
                                              " id_accountbalance: " + id_balance);
                                            trx.commit(transactionsResult);
                                          })
                                          .catch(function (error) {
                                            console.log("Updating transaction for id_accountbalance failed:", error);
                                            trx.rollback(error);
                                          });
                                        } else {
                                          let trCorr = self._makeTransactionObject(
                                            balance.balance - balanceAfterInsertingTransactions, balance.balanceDate,
                                            'Korrekturbuchung', undefined, undefined,
                                            undefined, undefined, undefined, undefined,
                                            undefined, undefined, modifiedBy, accountId, id_balance,
                                            idCategoryNotUsed, categoriesByName, categoriesById, undefined, undefined);
                                          delete trCorr.payeePayerAcctNo;
                                          delete trCorr.payeePayerBankCode;
                                          delete trCorr.payeePayerName;

                                          knex('transaction')
                                          .insert(trCorr)
                                          .transacting(trx)
                                          .then(function (results) {
                                            console.log("WARNING: Transaction to correct balance was added." +
                                              " id_accountbalance: " + id_balance);

                                            trx.commit(transactionsResult);
                                          })
                                          .catch(function (error) {
                                            console.log("Inserting transaction to correct balance failed:", error);
                                            trx.rollback(error);
                                          });
                                          // trx.rollback("Balance differs and corresponding transaction is missing");
                                        }
                                      })
                                      .catch(function (error) {
                                        console.log("Inserting into accountbalance failed:", error);
                                        trx.rollback(error);
                                      });

                                    } else {
                                      // todo update balance if it differs
                                      console.log(
                                        "Balance is already stored in database. Committing DB transaction without" +
                                        " adding a balance.");
                                      trx.commit(transactionsResult);
                                    }
                                  } else {
                                    // trx.rollback("Have balance for " + balanceDateStr + " but no transaction for that date");
                                    console.log("Have balance for " + balanceDateStr + " but no transaction for that date" +
                                      " -> ignore balance");
                                    trx.commit(transactionsResult);
                                  }
                                })
                                .catch(function (error) {
                                  console.log("Rollback transaction", error);
                                  trx.rollback(error);
                                });
                              }
                            })
                            .catch(function (error) {
                              trx.rollback(error);
                            });
                          })
                          .catch(function (err) {
                            console.log('failed to select just inserted transactions', err);
                            console.log("Rollback transaction");
                            trx.rollback(err);
                          });

                        })
                        .catch(function (error) {
                          console.log("Rollback transaction", error);
                          trx.rollback(error);
                        });
                      }).then(function (resp) {
                        console.log('Transaction complete.');
                        callback(null, resp);
                      }).catch(function (err) {
                        console.error("Transaction was rolled back", err);
                        callback(err);
                      });

                    }
                  });

                } else {
                  callback(null, []);
                }
              }).catch(function (err) {
                console.log('failed to select transactions', err);
                callback(err);
              });
            } catch (ex) {
              callback(ex);
            }
          }
        });
      }
    });
  }

}

module.exports = DB;
