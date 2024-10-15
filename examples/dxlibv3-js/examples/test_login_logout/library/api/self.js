import * as api from './core.js';
import '../../../../dxlibv3.js';

export async function self_prekey(keys) {
    const ed25519KeyPair = dxlibv3.Ed25519.keyPair();
    const edA0PublicKeyAsBytes = ed25519KeyPair.publicKey;
    keys.edA0PrivateKeyAsBytes = ed25519KeyPair.secretKey;

    const x25519KeyPair1 = dxlibv3.X25519.keyPair();
    const ecdhA1PublicKeyAsBytes = x25519KeyPair1.publicKey;
    keys.ecdhA1PrivateKeyAsBytes = x25519KeyPair1.secretKey;

    const x25519KeyPair2 = dxlibv3.X25519.keyPair();
    const ecdhA2PublicKeyAsBytes = x25519KeyPair2.publicKey;
    keys.ecdhA2PrivateKeyAsBytes = x25519KeyPair2.secretKey;

    // Convert keys to string
    const edA0PublicKeyAsHexString = dxlibv3.bytesToHex(edA0PublicKeyAsBytes);
    const ecdhA1PublicKeyAsHexString = dxlibv3.bytesToHex(ecdhA1PublicKeyAsBytes);
    const ecdhA2PublicKeyAsHexString = dxlibv3.bytesToHex(ecdhA2PublicKeyAsBytes);

    const pre_login_response = await api.APIPublic(keys,"/self/prekey", {
        a0: edA0PublicKeyAsHexString,
        a1: ecdhA1PublicKeyAsHexString,
        a2: ecdhA2PublicKeyAsHexString,
    }, true);

    const preLoginResponseDataAsJSON = await pre_login_response.json();

    keys.preKeyIndex = preLoginResponseDataAsJSON[`i`]
    const edB0PublicKeyAsHexString = preLoginResponseDataAsJSON[`b0`];
    const ecdhB1PublicKeyAsHexString = preLoginResponseDataAsJSON[`b1`];
    const ecdhB2PublicKeyAsHexString = preLoginResponseDataAsJSON[`b2`];
    keys.edB0PublicKeyAsBytes = dxlibv3.hexToBytes(edB0PublicKeyAsHexString);
    const ecdhB1PublicKeyAsBytes = dxlibv3.hexToBytes(ecdhB1PublicKeyAsHexString);
    const ecdhB2PublicKeyAsBytes = dxlibv3.hexToBytes(ecdhB2PublicKeyAsHexString);

    keys.sharedKey1AsBytes = dxlibv3.X25519.computeSharedSecret(keys.ecdhA1PrivateKeyAsBytes, ecdhB1PublicKeyAsBytes);
    keys.sharedKey2AsBytes = dxlibv3.X25519.computeSharedSecret(keys.ecdhA2PrivateKeyAsBytes, ecdhB2PublicKeyAsBytes);
    return pre_login_response;
}

export async function self_login(keys, userLogin, password) {
    await self_prekey(keys);

    const lvUserLogin = new dxlibv3.LV(userLogin);
    const lvPassword = new dxlibv3.LV(password);

    const dataBlockEnvelopeAsHexString = await dxlibv3.packLVPayload(keys.preKeyIndex, keys.edA0PrivateKeyAsBytes, keys.sharedKey1AsBytes, [lvUserLogin, lvPassword]);

    const login_response = await api.APIPublic(keys, "/self/login", {
        i: keys.preKeyIndex,
        d: dataBlockEnvelopeAsHexString,
    }, true);

    const loginResponseDataAsJSON = await login_response.json();
    const dataBlockEnvelopeAsHexString2 = loginResponseDataAsJSON['d']

    let lvPayloadElements = await dxlibv3.unpackLVPayload(keys.preKeyIndex, keys.edB0PublicKeyAsBytes, keys.sharedKey2AsBytes, dataBlockEnvelopeAsHexString2)

    let lvSessionObject = lvPayloadElements[0]

    let sessionObjectAsString = lvSessionObject.getValueAsString();
    console.log(sessionObjectAsString)

    let sessionObject = JSON.parse(sessionObjectAsString);
    console.log(sessionObject)

    keys.sessionKey = sessionObject['session_key'];
    keys.userId = sessionObject['user_id'];

    if (keys.sessionKey === "") {
        alert("Invalid resulted session key")
        return
    }

    console.log(keys.sessionKey, "logged in");
    return login_response
}

export async function self_logout(keys) {
    const logout_response = await api.APILogged(keys, "/self/logout", null, true);
    console.log(keys.sessionKey, "logged out")
    return logout_response;
}

export async function self_detail(keys) {
    return await api.APILogged(keys, "/self/detail", null, true);
}

export async function self_password_change(keys, new_password, old_password) {
    await self_prekey(keys);
    const lvNewPassword = new dxlibv3.LV(new_password);
    const lvOldPassword = new dxlibv3.LV(old_password);
    const dataBlockEnvelopeAsHexString = await dxlibv3.packLVPayload(keys.preKeyIndex, keys.edA0PrivateKeyAsBytes, keys.sharedKey1AsBytes, [lvNewPassword, lvOldPassword]);

    const response = await api.APILogged(keys, "/self/password/change", {
        "i": keys.preKeyIndex,
        "d": dataBlockEnvelopeAsHexString,
    }, true)
    console.log("Change Password user  done")
    return response;
}

