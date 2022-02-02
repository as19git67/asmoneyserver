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

  async isSchemaOK() {
    return await this._existsTable('XXX');
  }

  async makeSchemaUpToDate() {
    return await this._createTables();
  }

  async addDevice(deviceId, pubkey, privkey, registration) {
    const result = await this.knex('Devices').insert(
          {
            deviceid: deviceId,
            pubkey: pubkey,
            privkey: privkey,
            registration: registration.toDate(),
          }).returning('id');

    const id = result[0];
    return id;
  }
   
  async _createTables() {
    console.log('Creating database tables...');
    await this._dropall(['Devices', 'UsersAccessTokens', 'Users', 'Transactions', 'Accounts', 'FinTsContacts']);

    // CREATE TABLES

    try {
      let tableName = 'Devices';
      await this.knex.schema.createTable(tableName, function (t) {
          t.increments('id').primary();
          t.string('deviceid').unique().notNullable().index();
          t.dateTime('registration').notNullable().index();
          t.string('pubkey', 2048).unique().notNullable();
          t.string('privkey', 512).unique().notNullable();
      });
      console.log("Table " + tableName + " created");
    
      tableName = 'Users';
      await this.knex.schema.createTable(tableName, function (t) {
          t.increments('id').primary();
          t.string('username').unique().notNullable().index();
          t.string('passwordSalt').notNullable();
          t.string('passwordHash').notNullable();
          t.string('initials', 2);
      });
      console.log("Table " + tableName + " created");

      tableName = 'UsersAccessTokens';
      await this.knex.schema.createTable(tableName, function (t) {
        t.increments('id').primary();
        t.integer('idUser').notNullable().references('id').inTable('Users').index();
        t.string('client').notNullable().index();
        t.string('accessToken').notNullable().index();
        t.string('refreshToken').notNullable().index();
        t.integer('expiresIn').notNullable();
        t.dateTime('expiredAfter').notNullable().index();
      });
      console.log("Table " + tableName + " created");

      tableName = 'FinTsContacts';
      await this.knex.schema.createTable(tableName, function (t) {
        t.increments('id').primary();
        t.string('Name').unique().notNullable();
      });
      console.log("Table " + tableName + " created");

      await this._switchSystemVersioningOn('FinTsContacts');

      tableName = 'Accounts';
      await this.knex.schema.createTable(tableName, function (t) {
        t.increments('id').primary();
        t.string('name').notNullable().index();
      });
      console.log("Table " + tableName + " created");
      await this._switchSystemVersioningOn(tableName);

      tableName = 'Transactions';
      await this.knex.schema.createTable(tableName, function (t) {
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
                t.integer('idAccount').notNullable().references('id').inTable('Accounts').index();
        t.dateTime('transactionDate', false);   // = Buchungsdatum;  false means database does store timezone
      });
      console.log("Table " + tableName + " created");
      await this._switchSystemVersioningOn(tableName);
    }
    catch(ex) {
      console.log("creating table " + tableName + " failed");
      console.log(ex);
      throw ex;
    }  
  }

  async _existsTable(table, callback) {
    return await new Promise(async (resolve, reject) => {
      //    knex.raw("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME='" + table + "'").then(function (queryResult) {
      this.knex('INFORMATION_SCHEMA.TABLES').where({TABLE_NAME: table}).count('* as cnt').then(function (queryResult) {
        let cnt = queryResult[0].cnt;
        resolve(cnt > 0);
      }).catch(function (err) {
        console.log('Query to check whether table ' + table + ' exists failed');
        reject(err);
      });
    });
  }

  // Execute all functions in the array serially
  async _switchSystemVersioningOff(table) {
    await new Promise(async (resolve, reject) => {
      this.knex.raw('ALTER TABLE dbo.' + table + ' SET (SYSTEM_VERSIONING = OFF)').then(function () {
        console.log('System versioning switched OFF for table ' + table);
        resolve();
      }).catch(function (err) {
        if (err.number === 13591) {
          // ignore error when system versioning is not turned on
          resolve();
        } else {
          console.log('Switching system versioning off failed for table ' + table);
          reject(err);
        }
      });
    });
  }

  async _switchSystemVersioningOn(table) {
    await new Promise(async (resolve, reject) => {
      this.knex.raw('ALTER TABLE dbo.' + table +
        ' ADD SysStartTime datetime2 GENERATED ALWAYS AS ROW START NOT NULL, SysEndTime datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (SysStartTime,SysEndTime)').then(
        function () {
          console.log('System versioning switched ON for table ' + table);
          resolve();
        }).catch(function (err) {
        console.log('altering table ' + table + ' failed');
        reject(err);
      });
    });
  }

  async _dropall(tables) {
    if (!_.isArray(tables)) {
      throw new Error("tables argument must be an array with table names");
    }

    for (const table of tables) {
      try {
        const exists = await this._existsTable(table);
        if (exists) {
          await new Promise(async (resolve, reject) => {
            this.knex.raw('DROP TABLE dbo.' + table).then(function () {
              console.log('Table ' + table + ' dropped');
              resolve();
            }).catch(function (err) {
              console.log('dropping table ' + table + ' failed');
              reject(err);
            });
          });
        } else {
          console.log("Table " + table + " not dropping, because it does not exist.");
        }
      } catch (ex) {
        console.log("checking for table " + table + " failed");
        throw ex;
      }
    }
  }

  async getAccounts() {
    //&& ((a.Typ.HasValue && a.Typ == 1) || a.Typ.HasValue == false) && now < a.
    const queryResult = await this.knex.select().table('s_konten')
    .where(function () {
      this.where('deleted', false)
    })
    .andWhere(function () {
      this.where('geschlossen', '>', moment().toDate())
    });

    const accounts = _.map(queryResult, function (a) {
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
    return accounts;
  }

  _mapTransactionsFromDB(transactionsFromDB) {
    const transactions = _.map(transactionsFromDB, function (t) {
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

  async getTransactions(accountId, limit) {
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
      const queryResult = await this.knex.select().table('transaction')
      .where(where)
      .whereIn(whereIn)
      .whereNull('valid_end')
      .limit(limit)
      .orderBy('Datum', 'desc');

      const transactions = this._mapTransactionsFromDB(queryResult);
      return transactions;
    } else {
      const queryResult = this.knex.select().table('transaction')
      .where(where)
      .whereNull('valid_end')
      .limit(limit)
      .orderBy('Datum', 'desc');

      const transactions = self._mapTransactionsFromDB(queryResult);
      return transactions;
    }
  }

  async _getNotUsedCategoryId() {
    let self = this;
    if (this._idCategoryNotUsed !== undefined) {
      return this._idCategoryNotUsed;
    }
    const idCategoryNotUsed = this.KATEGORIE_VERWENDUNG.NO_CATEGORY;
    const queryResult = await this.knex.select().table('s_kategorieverwendung')
    .where({
      'id_kategorieverwendung': idCategoryNotUsed
    })
    .whereNotNull('id_category');

    if (_.isArray(queryResult) && queryResult.length > 0) {
      this._idCategoryNotUsed = queryResult[0].id_category;
      return this._idCategoryNotUsed;
    } else {
      return undefined;
    }
  }

  async _getCashAccountForUser(username) {
    console.log("Selecting user_preferences for " + username);
    const queryResult = await this.knex.select().table('user_preferences')
    .where({
      'Username': username
    })
    console.log("Results of query", queryResult);
    if (_.isArray(queryResult) && queryResult.length > 0) {
      return queryResult[0].pref_id_konto_for_cash; // idAccount
    } else {
      return undefined;
    }
  }

  async _createOrGetPayeePayerId(payeePayerName, payeePayerAcctNo, payeePayerBankCode) {
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

      const queryResult = await knex.select().table('zahlungsempfaenger')
      .where({
        'Name': name
      });

      if (_.isArray(queryResult) && queryResult.length > 0) {
        return queryResult[0].id_zahlungsempfaenger;
      } else {
        // insert as new
        const result = await knex('zahlungsempfaenger').insert(
          {
            Name: name,
            PayeePayerBankcode: payeePayerBankCode ? payeePayerBankCode : undefined,
            PayeePayerAccountnumber: payeePayerAcctNo ? payeePayerAcctNo : undefined
          })
        .returning('id_zahlungsempfaenger');

        return result[0]; // id_zahlungsempfaenger
      }
    } else {
      return; // can't search for no-string payee payer name
    }
  }

  async addTransactionsToUsersCashAccount(username, transactions) {
    const accountId = await this._getCashAccountForUser(username);
    if (accountId !== undefined) {
      await this.addTransactions(username, accountId, transactions, null);
    } else {
      throw new Error("no cash account configured for user with id " + userId);
    }
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

  async addTransactions(modifiedBy, accountId, transactions, balance) {
    if (_.isNaN(accountId)) {
      throw new Error("accountId must be a number");
    }
    let knex = this.knex;
    const idCategoryNotUsed = await this._getNotUsedCategoryId();
    const categoriesBy = await this._getCategories();

    let oldestDate = moment();
    let hashes = [];
    let coordinatesByHash = {};
    const transactionsToInsert = _.map(transactions, function (t) {

      let tr = this._makeTransactionObject(t.amount, t.valueDate, t.paymentPurpose, t.payeePayerAcctNo, t.payeePayerBankCode,
        t.payeePayerName, t.entryText, t.type, t.gvCode, t.primaNotaNo, t.category, modifiedBy, accountId, undefined,
        idCategoryNotUsed, categoriesBy.name, categoriesBy.id, null, t.bankTransactionId);

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

    const alreadySavedTransactions = await knex.select().table('transaction')
    .where({
      'deleted': false,
      'id_konto': accountId
    })
    .where('Datum', '>=', oldestDate.toDate())
    .whereNull('valid_end')
    .whereNotNull('md5hash');

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

      for (const t of filteredTransactionsToInsert) {
        t.id_zahlungsempfaenger = await this._createOrGetPayeePayerId(t.payeePayerName, t.payeePayerAcctNo, t.payeePayerBankCode);
        delete t.payeePayerName;
        delete t.payeePayerAcctNo;
        delete t.payeePayerBankCode;
      }

      let transact;
      try {
        await knex.transaction(async trx => {
          transact = trx;
          console.log("inserting transactions...");

          await knex.batchInsert('transaction', filteredTransactionsToInsert).transacting(trx);

          console.log("Select again after inserting transactions. Hashes:", hashes);

          const justSavedTransactions = await knex.select().table('transaction')
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
          .orderBy('id_transaction', 'desc');

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

          let transactionsResult = this._mapTransactionsFromDB(justSavedTransactions);

          console.log("Coordinates to add: " + coordinatesToAdd.length);

          await knex.batchInsert('transaction_location', coordinatesToAdd).transacting(trx);

          console.log("Coordinates inserted into DB.");

          // ### handle balance

          if (!balance || balance.balanceDate === undefined || balance.balance === undefined) {
            console.log("Have no balance to update. Committing DB transaction.");
            trx.commit(transactionsResult);
          } else {
            let balanceDate = moment(balance.balanceDate);
            // calculate balance by making the sum of all amounts in transactions for the account
            const results = await knex.raw(
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
            .transacting(trx);

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
                const result = await knex('accountbalance').insert(
                  {
                    id_konto: accountId,
                    Datum: balanceDate.toDate(),
                    Betrag: balance.balance,
                    downloaded: moment().toDate()
                  })
                .transacting(trx)
                .returning('id_balance');

                let id_balance = result[0];

                if (balance.balance === balanceAfterInsertingTransactions) {
                  let trIdForBalance = transactionAtBalanceDate.id_transaction;
                  const result = await knex('transaction').update({id_accountbalance: id_balance})
                  .where('id_transaction', trIdForBalance)
                  .andWhere('deleted', false)
                  .transacting(trx);

                  console.log("Transaction with id " + trIdForBalance + " updated with" +
                    " id_accountbalance: " + id_balance);
                  trx.commit(transactionsResult);
                } else {
                  let trCorr = this._makeTransactionObject(
                    balance.balance - balanceAfterInsertingTransactions, balance.balanceDate,
                    'Korrekturbuchung', undefined, undefined,
                    undefined, undefined, undefined, undefined,
                    undefined, undefined, modifiedBy, accountId, id_balance,
                    idCategoryNotUsed, categoriesByName, categoriesById, undefined, undefined);
                  delete trCorr.payeePayerAcctNo;
                  delete trCorr.payeePayerBankCode;
                  delete trCorr.payeePayerName;

                  const results = await knex('transaction')
                  .insert(trCorr)
                  .transacting(trx);

                  console.log("WARNING: Transaction to correct balance was added." +
                    " id_accountbalance: " + id_balance);

                  trx.commit(transactionsResult);
                }

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
          }
        });
      } catch (ex) {
        console.log("Rollback transaction", ex);
        transact.rollback(ex);
      }
    }
  }

}

module.exports = DB;
