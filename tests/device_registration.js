import Axios from "axios";

const fkApiBaseUrl = 'http://localhost:3000/api/devices';


async function register(fkApiBaseUrl, deviceId, pubkey, privkey) {
  const url = fkApiBaseUrl + '/devices';

  try {
    let data = {deviceid: deviceId, pubkey: pubkey, privkey: privkey};
    const response = await Axios.post(url, data, {headers: {'Content-Type': 'application/json'}});
    let savedDeviceId = response.data;
    return savedDeviceId;
  } catch (ex) {
    console.log(`ERROR while sending device registration to finanzkraft server: ${ex.response.status} ${ex.response.statusText}`);
    throw ex;
  }
}