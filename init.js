const config = require('./config');
const DB = require('./database');
const Users = require('./Users');
const _ = require('lodash');

module.exports = async function (app) {
  let adminUser = config.get('adminUser');
  if (!adminUser) {
    console.log("Not checking if initialization needed, because adminUser is not configured");
    return;
  }

  const database = new DB();

  const exists = await database.isSchemaOK();
  if (exists) {
    console.log("Database schema is ok. Not performing initial config.");
    return;
  } else {
    const initialAdminPassword = config.get('initialAdminPassword');
    if (initialAdminPassword) {
      try {
        await database.makeSchemaUpToDate();
      }
      catch(ex) {
        console.log("ERROR: Creating or upgrading database schema failed: ", ex);
        return;
      }
    } else {
      const errMsg = "ERROR: Not creating or upgrading database schema, because initialAdminPassword is not configured";
      console.log(errMsg);
      return;
    }
  }
};