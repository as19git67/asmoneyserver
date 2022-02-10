const Axios = require("axios");
const {v4: uuidv4} = require('uuid');
const crypto = require('crypto');

const fkApiBaseUrl = 'http://localhost:3000/api';


async function register(fkApiBaseUrl, deviceId, pubkey, privkey) {
  const url = fkApiBaseUrl + '/devices';

  try {
    let data = {deviceid: deviceId, pubkey: pubkey, privkey: privkey};
    const response = await Axios.post(url, data, {headers: {'Content-Type': 'application/json'}});
    let savedDeviceId = response.data;
    return savedDeviceId;
  } catch (ex) {
    if (ex.response) {
      console.log(`ERROR while sending device registration to finanzkraft server: ${ex.response.status} ${ex.response.statusText}`);
    }
    throw ex;
  }
}

async function createNewKeyPair() {
  try {
    return await new Promise((resolve, reject) => {
      // create new RSA keypair
      crypto.generateKeyPair('rsa', {
        modulusLength: 4096,
        publicKeyEncoding: {
          type: 'spki',
          format: 'pem'
        },
        privateKeyEncoding: {
          type: 'pkcs8',
          format: 'pem',
          cipher: 'aes-256-cbc',
          passphrase: ''
        }
      }, async (err, publicKey, privateKey) => {
        if (err) {
          reject(err);
          return;
        }


        // // create new AES256 encryption key
        // const aesKeyAsBase64 = crypto.randomBytes(32).toString('base64');
        // // encrypt the encryption key with pwHash
        // const aesKeySecured = _encrypt(aesKeyAsBase64, pwHash, iv);
        //console.log(`pubkey: ${publicKey}, privkey: ${privateKey}`);
        console.log(`pubkey: ${publicKey.length}, privkey: ${privateKey.length}`);
        resolve({pubkey: publicKey, privkey: privateKey});
      });
    });
  } catch (ex) {
    console.log(`Generating keypair failed`);
    throw ex;
  }
}

async function createAccount(fkApiBaseUrl, deviceId, iban, bankContact, publicKey, privateKey) {
  if (!bankContact.username) {
    throw new Error('username missing in bankContact');
  }
  if (!bankContact.password) {
    throw new Error('password missing in bankContact');
  }
  const encryptedUsername = crypto.publicEncrypt(publicKey, Buffer.from(bankContact.username)).toString('base64');
  const encryptedPassword = crypto.publicEncrypt(publicKey, Buffer.from(bankContact.password)).toString('base64');

  const url = fkApiBaseUrl + '/accounts';

  // create a signature from the deviceId
  const data = Buffer.from(deviceId);
  const signature = crypto.sign("SHA256", data, {
    key: privateKey.toString(),
    passphrase: '',
  }).toString('base64');

  try {
    let data = {
      deviceid: deviceId,
      signature: signature,
      iban: iban,
      bankcontact: {
        type: bankContact.type,
        url: bankContact.url,
        username_enc: encryptedUsername,
        password_enc: encryptedPassword
      }
    };
    const response = await Axios.post(url, data, {headers: {'Content-Type': 'application/json'}});
    let savedAccountId = response.data;
    return savedAccountId;
  } catch (ex) {
    if (ex.response) {
      console.log(`ERROR while setting up a new account at the finanzkraft server: ${ex.response.status} ${ex.response.statusText}`);
    }
    throw ex;
  }
}

new Promise(async (resolve, reject) => {
  try {
    const key = await createNewKeyPair();
    const deviceId = uuidv4();

    await register(fkApiBaseUrl, deviceId, key.pubkey, key.privkey);

    const iban = 'DE82720691550000544604';
    const bankcontact = {
      type: 'fints',
      url: 'https://hbci11.fiducia.de/cgi-bin/hbciservlet',
      username: 'abc',
      password: 'secret'
    };
    await createAccount(fkApiBaseUrl, deviceId, iban, bankcontact, key.pubkey, key.privkey);
    resolve();
  } catch (ex) {
    reject(ex);
  }
}).then(() => {
  console.log("Test was successful");
}).catch((reason) => {
  console.log("Test failed");
  console.log(reason);
});
