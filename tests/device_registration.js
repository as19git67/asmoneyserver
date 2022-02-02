const Axios = require("axios");
const { v4: uuidv4 } = require('uuid');
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
  } catch(ex) {
    console.log(`Generating keypair failed`);
    throw ex;
  }
}

new Promise(async (resolve, reject) => {
  try {
    const key = await createNewKeyPair();

    await register(fkApiBaseUrl, uuidv4(), key.pubkey, key.privkey);
    resolve();
  }
  catch(ex) {
    reject(ex);
  }
}).then(() => {
    console.log("Test was successful");
}).catch((reason) => {
    console.log("Test failed");
    console.log(reason);
});
