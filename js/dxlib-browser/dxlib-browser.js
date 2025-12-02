import './libs/tweetnacl/nacl.js';

const dxlib = {};

(function (dxlib) {
    'use strict';


    function assertResponse(response) {
        let statusCode = response.status;
        if (statusCode !== 200) {
            alert(`${response.url}: Status code is ${statusCode.toString()}`);
            throw new Error("Execution halted");
        }
    }

    class InternalVariables {
        APIAddress = "";
        sessionKey = "";
        preKeyUrl = "";
    }

    async function api(internalVariables, url, jsonRequestData, asserted) {
        let bodyAsString = "";
        if (jsonRequestData !== null) {
            bodyAsString = JSON.stringify(jsonRequestData);
        }

        let headers = {
            'Content-Type': 'application/json',
        }
        if (internalVariables.sessionKey !== "") {
            headers["Authorization"] = `Bearer ${internalVariables.sessionKey}`;
        }
        let response = await fetch(internalVariables.APIAddress + url, {
            method: 'POST',
            headers: headers,
            body: bodyAsString,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    async function postJSON(internalVariables, url, headers, jsonRequestData, asserted) {
        let bodyAsString = "";
        if (jsonRequestData !== null) {
            bodyAsString = JSON.stringify(jsonRequestData);
        }

        if (headers == null) {
            headers = {
                'Content-Type': 'application/json',
            }
        }


        let response = await fetch(internalVariables.APIAddress + url, {
            method: 'POST',
            headers: headers,
            body: bodyAsString,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    async function apiUpload(internalVariables, url, content_type, parameters, fileContent, asserted) {
        let headers = {
            'Content-Type': content_type,
        }
        if (internalVariables.sessionKey !== null) {
            if (internalVariables.sessionKey !== "") {
                headers["Authorization"] = `Bearer ${internalVariables.sessionKey}`;
            }
        }
        if (parameters !== null) {
            headers["X-Var"] = JSON.stringify(parameters);
        }
        let response = await http.post(internalVariables.APIAddress + url, fileContent, {
            headers: headers,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    async function apiPrekey(internalVariables) {
        //console.log("1a", new Date().toISOString());
        const ed25519KeyPair = dxlib.Ed25519.keyPair();
        const edA0PublicKeyAsBytes = ed25519KeyPair.publicKey;
        internalVariables.edA0PrivateKeyAsBytes = ed25519KeyPair.secretKey;

        const x25519KeyPair1 = dxlib.X25519.keyPair();
        const ecdhA1PublicKeyAsBytes = x25519KeyPair1.publicKey;
        internalVariables.ecdhA1PrivateKeyAsBytes = x25519KeyPair1.secretKey;

        const x25519KeyPair2 = dxlib.X25519.keyPair();
        const ecdhA2PublicKeyAsBytes = x25519KeyPair2.publicKey;
        internalVariables.ecdhA2PrivateKeyAsBytes = x25519KeyPair2.secretKey;

        // Convert keys to string
        const edA0PublicKeyAsHexString = dxlib.bytesToHex(edA0PublicKeyAsBytes);
        const ecdhA1PublicKeyAsHexString = dxlib.bytesToHex(ecdhA1PublicKeyAsBytes);
        const ecdhA2PublicKeyAsHexString = dxlib.bytesToHex(ecdhA2PublicKeyAsBytes);
       // console.log("1b", new Date().toISOString());

        const pre_login_response = await dxlib.api(internalVariables, internalVariables.preKeyUrl, {
            a0: edA0PublicKeyAsHexString,
            a1: ecdhA1PublicKeyAsHexString,
            a2: ecdhA2PublicKeyAsHexString,
        }, true);
        //console.log("1c", new Date().toISOString());
        if (pre_login_response.status !== 200) {
            return pre_login_response;
        }

        const preLoginResponseDataAsJSON = await pre_login_response.json();

        internalVariables.preKeyIndex = preLoginResponseDataAsJSON["i"]
        const edB0PublicKeyAsHexString = preLoginResponseDataAsJSON["b0"];
        const ecdhB1PublicKeyAsHexString = preLoginResponseDataAsJSON["b1"];
        const ecdhB2PublicKeyAsHexString = preLoginResponseDataAsJSON["b2"];
        internalVariables.edB0PublicKeyAsBytes = dxlib.hexToBytes(edB0PublicKeyAsHexString);
        const ecdhB1PublicKeyAsBytes = dxlib.hexToBytes(ecdhB1PublicKeyAsHexString);
        const ecdhB2PublicKeyAsBytes = dxlib.hexToBytes(ecdhB2PublicKeyAsHexString);
        //console.log("1d", new Date().toISOString());
        internalVariables.sharedKey1AsBytes = dxlib.X25519.computeSharedSecret(internalVariables.ecdhA1PrivateKeyAsBytes, ecdhB1PublicKeyAsBytes);
        //console.log("1e", new Date().toISOString());
        internalVariables.sharedKey2AsBytes = dxlib.X25519.computeSharedSecret(internalVariables.ecdhA2PrivateKeyAsBytes, ecdhB2PublicKeyAsBytes);
        //console.log("1f", new Date().toISOString());
        return pre_login_response;
    }
    // Helper function definition for converting Base64 to Hex string.
    // It uses codePointAt(0) to correctly handle all binary characters (which shouldn't be
    // surrogate pairs in this binary context, but it satisfies the SonarQube rule and avoids issues).
    function b64ToHex(b64) {
        // Decodes base64 string to a raw binary string
        const binaryString = atob(b64);

        // Uses codePointAt(0) to extract the character code safely, then converts to hex
        return Array.from(binaryString, char =>
            char.codePointAt(0).toString(16).padStart(2, '0')
        ).join('');
    }

    async function apiSecureV2(internalVariables, url, header, parameters, asserted) {
        let response = await apiPrekey(internalVariables);
        if (response.status !== 200) {
            return response;
        }
        if (header==null) {
            header = {}
        }
        if (internalVariables.sessionKey !== null) {
            header["Authorization"] = `Bearer ${internalVariables.sessionKey}`;
        }
        const headerAsJsonString = JSON.stringify(header);
        const headerAsJSONStringAsBytes = new TextEncoder().encode(headerAsJsonString);
        const headerAsJSONStringAsBinaryString = String.fromCodePoint(...headerAsJSONStringAsBytes);

        // Encode the resulting binary string to Base64
        const headerAsJSONStringAsBase64 = btoa(headerAsJSONStringAsBinaryString);

        const parametersAsJsonString = JSON.stringify(parameters);
        const parametersAsJSONStringAsBytes = new TextEncoder().encode(parametersAsJsonString);
        const parametersAsJSONStringAsBinaryString = String.fromCodePoint(...parametersAsJSONStringAsBytes);

        // Encode the resulting binary string to Base64
        const parametersAsJSONStringAsBase64 = btoa(parametersAsJSONStringAsBinaryString);

        const lvHeader = new dxlib.LV(headerAsJSONStringAsBase64);
        const lvParameters = new dxlib.LV(parametersAsJSONStringAsBase64);

        // FIX: Replaced 'keys' with 'internalVariables'
        const dataBlockEnvelopeAsHexString = await dxlib.packLVPayload(internalVariables.preKeyIndex, internalVariables.edA0PrivateKeyAsBytes, internalVariables.sharedKey1AsBytes, [lvHeader, lvParameters]);
        //console.log("3", new Date().toISOString());

        const rawResponse = await dxlib.postJSON(internalVariables, url, null,{
            i: internalVariables.preKeyIndex,
            d: dataBlockEnvelopeAsHexString,
        }, asserted);

        if (rawResponse.status !== 200) {
            return rawResponse;
        }
       // console.log("4", new Date().toISOString());

        const loginResponseDataAsJSON = await rawResponse.json();
        const dataBlockEnvelopeAsHexString2 = loginResponseDataAsJSON['d']

        // FIX: Replaced 'keys' with 'internalVariables'
        let lvPayloadElements = await dxlib.unpackLVPayload(internalVariables.preKeyIndex, internalVariables.edB0PublicKeyAsBytes, internalVariables.sharedKey2AsBytes, dataBlockEnvelopeAsHexString2)
        //console.log("5", new Date().toISOString());

        const lvPayLoadStatusCode = lvPayloadElements[0];
        const lvPayLoadHeader = lvPayloadElements[1];
        const lvPayLoadBody = lvPayloadElements[2];


        // 1. Decode and Reconstruct Status Code
        const statusCodeBase64 = lvPayLoadStatusCode.getValueAsString();
        // FIX: Replaced complex map/join with the helper function b64ToHex
        const statusCodeBytes = dxlib.hexToBytes(b64ToHex(statusCodeBase64));
        const statusCodeView = new DataView(statusCodeBytes.buffer);
        const originalStatusCodeBigInt = statusCodeView.getBigUint64(0, false);

        // Since HTTP status codes are always 3-digit integers (e.g., 200, 404)
        // and fit within a standard number (uint32), we convert it back to a standard JS number.
        // If the actual statusCode was a very large 64-bit number, it would need to remain a BigInt.
        const originalStatusCode = Number(originalStatusCodeBigInt);

        // 2. Decode and Reconstruct Header
        const headerBase64 = lvPayLoadHeader.getValueAsString();
        // FIX: Replaced complex map/join with the helper function b64ToHex
        const headerJsonString = new TextDecoder().decode(dxlib.hexToBytes(b64ToHex(headerBase64)));
        let originalHeader;
        try {
            originalHeader = JSON.parse(headerJsonString);
        } catch (e) {
            console.error("Error parsing header JSON:", e);
            originalHeader = {};
        }

        // 3. Decode and Reconstruct Body
        const bodyBase64 = lvPayLoadBody.getValueAsString();
        // FIX: Replaced complex map/join with the helper function b64ToHex
        const originalBodyAsBytes = dxlib.hexToBytes(b64ToHex(bodyBase64));
        const originalBodyAsString = new TextDecoder().decode(originalBodyAsBytes);

        // PUT THE CODE HERE
        // Create a Headers object from the decrypted header map
        const headers = new Headers(originalHeader);

        // Create a synthetic Response object using the decrypted data
        const decryptedResponse = new Response(originalBodyAsString, {
            status: originalStatusCode,
            statusText: originalStatusCode.toString(),
            headers: headers,
            url: rawResponse.url,
            ok: originalStatusCode >= 200 && originalStatusCode <= 299,
        });

        decryptedResponse.originalBodyAsBytes = originalBodyAsBytes;

        return decryptedResponse;
    }

    class Ed25519 {
        static keyPair() {
            return nacl.sign.keyPair();
        }

        static sign(msg, selfPrivateKey) {
            return nacl.sign.detached(msg, selfPrivateKey)

        }

        static verify(msg, signature, peerPublicKey) {
            return nacl.sign.detached.verify(msg, signature, peerPublicKey);
        }
    }

    class X25519 {
        static keyPair() {
            return nacl.box.keyPair();
        }

        static computeSharedSecret(privateAKey, publicBKey) {
            // Ensure the privateKey and publicKey are Uint8Arrays
            if (!(privateAKey instanceof Uint8Array) || !(publicBKey instanceof Uint8Array)) {
                throw new TypeError('Both keys must be Uint8Arrays');
            }

            return nacl.scalarMult(privateAKey, publicBKey)
        }
    }

    function toUint8Array(data) {
        // If already a Uint8Array, return it
        if (data instanceof Uint8Array) {
            return data;
        }

        // If it's an ArrayBuffer, create a view of it
        if (data instanceof ArrayBuffer) {
            return new Uint8Array(data);
        }

        // If it's a string, encode it
        if (typeof data === 'string') {
            return new TextEncoder().encode(data);
        }

        // If it's a number (assuming 32-bit integer)
        if (typeof data === 'number') {
            const arr = new Uint8Array(4);
            new DataView(arr.buffer).setInt32(0, data, true);
            return arr;
        }

        // If it's a BigInt (64-bit integer)
        if (typeof data === 'bigint') {
            const arr = new Uint8Array(8);
            new DataView(arr.buffer).setBigInt64(0, data, true);
            return arr;
        }

        // If it's an array-like object
        if (Array.isArray(data) || ArrayBuffer.isView(data)) {
            return new Uint8Array(data);
        }

        // If it's an object, stringify it and then encode
        if (typeof data === 'object') {
            return new TextEncoder().encode(JSON.stringify(data));
        }

        // If we can't handle the input, throw an error
        throw new Error('Unsupported data type');
    }

    class LV {
        Value;
        Length;

        constructor(value) {
            this.setValue(value)
        }

        static unmarshalBinary(data) {
            if (!(data instanceof Uint8Array)) {
                if (!Array.isArray(data)) {
                    data = [data];
                }
                data = new Uint8Array(data);
            }
            let dataArray = new DataView(data.buffer);
            let l = dataArray.getInt32(0, false);
            let v = new Uint8Array(data.slice(4, 4 + l));
            return new LV(v);
        }

        static combine(lvs) {
            if (!Array.isArray(lvs)) {
                lvs = [lvs];
            }
            let totalLength = 0
            let lvAsBytesArray = [];
            for (const element of lvs) {
                /** @type {LV} */
                let t = element
                let b = t.marshalBinary()
                lvAsBytesArray.push(b)
                totalLength = totalLength + b.length
            }

            let r = new Uint8Array(totalLength)
            let o = 0;
            for (let i = 0; i < lvs.length; i++) {
                r.set(lvAsBytesArray[i], o)
                o = o + lvAsBytesArray[i].length
            }
            return new LV(r)
        }

        setValue(value) {
            let t = toUint8Array(value)
            this.Value = new Uint8Array(t);
            this.Length = this.Value.length
        }

        setValueAsString(valueAsString) {
            const encoder = new TextEncoder();
            const valueAsBytes = encoder.encode(valueAsString);
            this.setValue(valueAsBytes)
        }

        getValueAsString() {
            const decoder = new TextDecoder();
            return decoder.decode(this.Value);
        }

        marshalBinary() {
            let bufferLength = 4 + this.Value.length;

            let buffer = new ArrayBuffer(bufferLength);
            let dataView = new DataView(buffer);

            // Write Length as int32 in BigEndian byte order
            dataView.setUint32(0, this.Length, false);

            // Create a new Uint8Array view for the buffer
            let thisAsBytes = new Uint8Array(buffer);

            // Copy Value into thisAsBytes
            thisAsBytes.set(this.Value, 4);

            return thisAsBytes;
        }

        expand() {
            let data = this.Value;
            let dataArray = new DataView(data.buffer);

            let r = [];
            let i = 0;
            let j = 0;
            while (i < this.Value.length) {
                let l = dataArray.getInt32(i, false)
                i = i + 4;
                j = i + l;
                let v = this.Value.subarray(i, j)
                let e = new LV(v)
                r.push(e)
                i = j;
            }
            return r
        }
    }

    class DataBlock {
        Time = new LV({});
        Nonce = new LV({});
        PreKey = new LV({});
        Data = new LV({});
        DataHash = new LV({});

        constructor(data) {
            this.setTimeNow();
            this.generateNonce();
            if (data !== undefined) {
                this.setDataValue(data);
            }
        }

        /** @param {LV} aLV */
        static fromLV(aLV) {
            let lvs = aLV.expand()
            let db = new DataBlock()
            db.Time = lvs[0];
            db.Nonce = lvs[1];
            db.PreKey = lvs[2];
            db.Data = lvs[3];
            db.DataHash = lvs[4];
            return db;
        }

        setTimeNow() {
            let now = new Date();
            let currentTimeInUTC_ISOFormat = now.toISOString();
            console.log(currentTimeInUTC_ISOFormat);
            this.Time.setValueAsString(currentTimeInUTC_ISOFormat);
        }

        generateNonce() {
            this.Nonce.setValue(nacl.randomBytes(32));
        }

        setDataValue(data) {
            this.Data.setValue(data)
            this.generateDataHash()
        }

        generateDataHash() {
            let dataAsBytes = this.Data.Value
            let hash = nacl.hash(dataAsBytes)
            this.DataHash.setValue(hash)
        }

        checkDataHash() {
            let dataAsBytes = this.Data.Value
            let dataHashAsBytes = this.DataHash.Value
            let hash = nacl.hash(dataAsBytes)
            return compareByteArrays(hash, dataHashAsBytes)
        }

        asLV() {
            return LV.combine([this.Time, this.Nonce, this.PreKey, this.Data, this.DataHash]);
        }
    }

    class AES {
        static async encrypt(key, data) {
            let decodedKey;
            let iv;
            let ciphertextArrayBuffer;
            let ciphertextArray;
            let resultArray;
            try {
                decodedKey = await globalThis.crypto.subtle.importKey(
                    "raw",
                    key.buffer,
                    "AES-CBC",
                    false,
                    ["encrypt", "decrypt"]
                );

                iv = globalThis.crypto.getRandomValues(new Uint8Array(16));
                ciphertextArrayBuffer = await globalThis.crypto.subtle.encrypt(
                    {
                        name: "AES-CBC",
                        iv: iv.buffer
                    },
                    decodedKey,
                    data.buffer
                );

                // Create new TypedArray for the IV and ciphertext
                ciphertextArray = new Uint8Array(ciphertextArrayBuffer);
                resultArray = new Uint8Array(iv.length + ciphertextArray.length);

                // Insert the IV and ciphertext into the result array
                resultArray.set(iv);
                resultArray.set(ciphertextArray, iv.length);
            } catch (err) {
                console.log(err)
                throw err;
            }
            return resultArray;
        }

        static async decrypt(key, encrypted) {
            const iv = encrypted.slice(0, 16); // get the IV from the first 16 bytes
            const data = encrypted.slice(16); // get the actual encrypted data
            let decodedKey
            let decryptedArrayBuffer
            try {
                decodedKey = await globalThis.crypto.subtle.importKey(
                    "raw",
                    key.buffer,
                    {name: "AES-CBC"},
                    false,
                    ["encrypt", "decrypt"] //only allow the key to be used for decryption
                );

                decryptedArrayBuffer = await globalThis.crypto.subtle.decrypt(
                    {
                        name: "AES-CBC",
                        iv: iv.buffer
                    },
                    decodedKey,
                    data.buffer
                )
            } catch (err) {
                console.log(err);
                throw (err);
            }

            return new Uint8Array(decryptedArrayBuffer);
        }
    }

    async function packLVPayload(preKeyIndex, edSelfPrivateKey, encryptKey, arrayOfLvParams) {
        let lvPackedPayload = dxlib.LV.combine(arrayOfLvParams);
        let lvPackedPayloadAsBytes = lvPackedPayload.marshalBinary();

        let dataBlock = new dxlib.DataBlock(lvPackedPayloadAsBytes);
        dataBlock.PreKey.setValue(preKeyIndex);
        let lvDataBlock = dataBlock.asLV();
        let lvDataBlockAsBytes = lvDataBlock.marshalBinary();

        let encryptedLVDataBlockAsBytes = await dxlib.AES.encrypt(encryptKey, lvDataBlockAsBytes)
        let lvEncryptedLVDataBlockAsBytes = new dxlib.LV(encryptedLVDataBlockAsBytes)
        let signature = Ed25519.sign(encryptedLVDataBlockAsBytes, edSelfPrivateKey)
        let lvSignature = new dxlib.LV(signature)
        let lvDataBlockEnvelope = dxlib.LV.combine([lvEncryptedLVDataBlockAsBytes, lvSignature])
        let lvDataBlockEnvelopeAsBytes = lvDataBlockEnvelope.marshalBinary()
        return bytesToHex(lvDataBlockEnvelopeAsBytes)
    }

    const UNPACK_TTL_MS = 5 * 60 * 1000;

    async function unpackLVPayload(preKeyIndex, peerPublicKey, decryptKey, dataAsHexString, skipVerify = false) {
        let dataAsBytes;
        let lvData;
        let lvDataElements
        let decryptedData;
        let lvDecryptedLVDataBlock;
        let dataBlockPreKeyIndex;
        let lvPtrDataPayload;
        let lvCombinedPayloadAsBytes;
        let lvCombinedPayload;
        let valid;
        let dataBlock;

        dataAsBytes = hexToBytes(dataAsHexString);

        lvData = LV.unmarshalBinary(dataAsBytes);

        /** @type {[LV]} */
        lvDataElements = lvData.expand();

        if (lvDataElements === null) {
            throw new Error('INVALID_DATA');
        }

        if (lvDataElements.length < 2) {
            throw new Error('INVALID_DATA');
        }

        /** @type {LV} */
        let lvEncryptedData = lvDataElements[0];
        /** @type {LV} */
        let lvSignature = lvDataElements[1];

        if (!skipVerify) {
            valid = Ed25519.verify(lvEncryptedData.Value, lvSignature.Value, peerPublicKey);
            if (!valid) {
                throw new Error('INVALID_SIGNATURE');
            }
        }

        decryptedData = await AES.decrypt(decryptKey, lvEncryptedData.Value);

        lvDecryptedLVDataBlock = LV.unmarshalBinary(decryptedData);

        dataBlock = DataBlock.fromLV(lvDecryptedLVDataBlock)

        let timeStamp = dataBlock.Time.getValueAsString();
        let parsedTimestamp = new Date(timeStamp)


        if (parsedTimestamp.toString() === 'Invalid Date') {
            throw new Error("INVALID_TIMESTAMP_DATA");
        }

        const differenceMS = Date.now() - parsedTimestamp
        if ((differenceMS - UNPACK_TTL_MS) > 0) {
            throw new Error("TIME_EXPIRED")
        }

        dataBlockPreKeyIndex = dataBlock.PreKey.getValueAsString();
        if (dataBlockPreKeyIndex !== preKeyIndex) {
            throw new Error('INVALID_PREKEY');
        }

        if (!dataBlock.checkDataHash()) {
            throw new Error('INVALID_DATA_HASH');
        }

        lvCombinedPayloadAsBytes = dataBlock.Data.Value;

        lvCombinedPayload = LV.unmarshalBinary(lvCombinedPayloadAsBytes);
        lvPtrDataPayload = lvCombinedPayload.expand();

        return lvPtrDataPayload;

    }

    function bytesToHex(bytes) {
        return Array.from(bytes, byte => {
            // Ensure byte is treated as a number
            let num = Number(byte);
            // Check if it's a valid number
            if (Number.isNaN(num)) {
                throw new TypeError('Invalid byte value');
            }
            return num.toString(16).padStart(2, '0');
        }).join('');
    }

    function hexToBytes(hex) {
        if (hex.length % 2 !== 0) {
            throw new Error('Hex string must have an even length');
        }
        const bytes = new Uint8Array(hex.length / 2);
        for (let i = 0; i < hex.length; i += 2) {
            let s = hex.substring(i, i + 2);
            bytes[i / 2] = Number.parseInt(s, 16);
        }
        return bytes;
    }


    function compareByteArrays(arr1, arr2) {
        if (arr1.length !== arr2.length) {
            return false;
        }
        for (let i = 0; i < arr1.length; i++) {
            if (arr1[i] !== arr2[i]) {
                return false;
            }
        }
        return true;
    }

    dxlib.Ed25519 = Ed25519;
    dxlib.X25519 = X25519;
    dxlib.LV = LV;
    dxlib.DataBlock = DataBlock;
    dxlib.AES = AES;
    dxlib.packLVPayload = packLVPayload;
    dxlib.unpackLVPayload = unpackLVPayload;
    dxlib.bytesToHex = bytesToHex;
    dxlib.hexToBytes = hexToBytes;
    dxlib.assertResponse = assertResponse;
    dxlib.api = api;
    dxlib.apiUpload = apiUpload;
    dxlib.apiPrekey = apiPrekey;
    dxlib.apiSecureV2 = apiSecureV2
    dxlib.postJSON = postJSON;
}) (dxlib);

if (typeof module !== 'undefined' && module.exports) {
    module.exports = dxlib;
} else {
    globalThis.dxlib = globalThis.dxlib || dxlib;
}

export default dxlib;
