import nacl from './libs/nacl.js';
import crypto from 'k6/crypto';
import CryptoJS from './libs/crypto-js-4.2.0/crypto-js.js';

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
    class Client {
        constructor(apiAddress, preKeyUrl, preKeyCaptcha) {
            this.APIAddress = apiAddress;
            this.preKeyUrl = preKeyUrl;
            this.preKeyCaptchaUrl = preKeyCaptcha;
            this.edA0PrivateKeyAsBytes = null;
            this.ecdhA1PrivateKeyAsBytes = null;
            this.ecdhA2PrivateKeyAsBytes = null;
            this.edB0PublicKeyAsBytes = null;
            this.sharedKey1AsBytes = null;
            this.sharedKey2AsBytes = null;
            this.preKeyIndex = null;
            this.sessionKey = null;
            this.userId = null;
        }
        Clone() {
            return new Keys(this.APIAddress, this.preKeyUrl);
        }
    }

    async function api(client, url, jsonRequestData, asserted) {
        let bodyAsString = "";
        if (jsonRequestData !== null) {
            bodyAsString = JSON.stringify(jsonRequestData);
        }

        let headers = {
            'Content-Type': 'application/json',
        }
        if (client.sessionKey !== "") {
            headers["Authorization"] = `Bearer ${client.sessionKey}`;
        }
        let response = await fetch(client.APIAddress + url, {
            method: 'POST',
            headers: headers,
            body: bodyAsString,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    async function postJSON(client, url, headers, jsonRequestData, asserted) {
        let bodyAsString = "";
        if (jsonRequestData !== null) {
            bodyAsString = JSON.stringify(jsonRequestData);
        }

        if (headers == null) {
            headers = {
                'Content-Type': 'application/json',
            }
        }

        let response = await fetch(client.APIAddress + url, {
            method: 'POST',
            headers: headers,
            body: bodyAsString,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    async function apiUpload(client, url, content_type, parameters, fileContent, asserted) {
        let headers = {
            'Content-Type': content_type,
        }
        if (client.sessionKey !== null) {
            if (client.sessionKey !== "") {
                headers["Authorization"] = `Bearer ${client.sessionKey}`;
            }
        }
        if (parameters !== null) {
            headers["X-Var"] = JSON.stringify(parameters);
        }
        let response = await http.post(client.APIAddress + url, fileContent, {
            headers: headers,
        });
        if (asserted) {
            assertResponse(response);
        }
        return response;
    }

    function b64ToHex(b64) {
        // Decodes base64 string to a raw binary string
        const binaryString = atob(b64);

        // Uses codePointAt(0) to extract the character code safely, then converts to hex
        return Array.from(binaryString, char =>
            char.codePointAt(0).toString(16).padStart(2, '0')
        ).join('');
    }

    function encodeUTF8(str) {
        let pos = 0;
        const len = str.length;
        let out = [];

        for (let i = 0; i < len; i++) {
            let point = str.codePointAt(i);

            // Skip the low surrogate if we just processed a high surrogate
            if (point > 0xFFFF) {
                i++; // Skip the next char as it's part of the surrogate pair
            }

            if (point <= 0x7F) {
                out[pos++] = point;
            } else if (point <= 0x7FF) {
                out[pos++] = 0xC0 | (point >>> 6);
                out[pos++] = 0x80 | (point & 0x3F);
            } else if (point <= 0xFFFF) {
                out[pos++] = 0xE0 | (point >>> 12);
                out[pos++] = 0x80 | ((point >>> 6) & 0x3F);
                out[pos++] = 0x80 | (point & 0x3F);
            } else if (point <= 0x10FFFF) {
                out[pos++] = 0xF0 | (point >>> 18);
                out[pos++] = 0x80 | ((point >>> 12) & 0x3F);
                out[pos++] = 0x80 | ((point >>> 6) & 0x3F);
                out[pos++] = 0x80 | (point & 0x3F);
            }
        }

        return new Uint8Array(out);
    }

    function decodeUTF8(bytes) {
        let out = '';
        let pos = 0;
        const len = bytes.length;

        while (pos < len) {
            const byte1 = bytes[pos++];

            if (byte1 <= 0x7F) {
                // 1-byte sequence (ASCII)
                out += String.fromCodePoint(byte1);
            } else if (byte1 <= 0xDF) {
                // 2-byte sequence
                const byte2 = bytes[pos++];
                const codePoint = ((byte1 & 0x1F) << 6) | (byte2 & 0x3F);
                out += String.fromCodePoint(codePoint);
            } else if (byte1 <= 0xEF) {
                // 3-byte sequence
                const byte2 = bytes[pos++];
                const byte3 = bytes[pos++];
                const codePoint = ((byte1 & 0x0F) << 12) | ((byte2 & 0x3F) << 6) | (byte3 & 0x3F);
                out += String.fromCodePoint(codePoint);
            } else if (byte1 <= 0xF7) {
                // 4-byte sequence
                const byte2 = bytes[pos++];
                const byte3 = bytes[pos++];
                const byte4 = bytes[pos++];
                const codePoint = ((byte1 & 0x07) << 18) | ((byte2 & 0x3F) << 12) | ((byte3 & 0x3F) << 6) | (byte4 & 0x3F);
                out += String.fromCodePoint(codePoint);
            }
        }

        return out;
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

        // If it's a string, encode it using our custom UTF-8 encoder
        if (typeof data === 'string') {
            return encodeUTF8(data);
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
            return encodeUTF8(JSON.stringify(data));
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
                let b = element.marshalBinary()
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
            this.setValue(encodeUTF8(valueAsString));
        }

        getValueAsString() {
            return decodeUTF8(this.Value);
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

    dxlib.LV = LV;
    dxlib.Client = Client;
    dxlib.bytesToHex = bytesToHex;
    dxlib.hexToBytes = hexToBytes;
    dxlib.compareByteArrays = compareByteArrays;
    dxlib.assertResponse = assertResponse;
    dxlib.api = api;
    dxlib.apiUpload = apiUpload;
    dxlib.b64ToHex = b64ToHex;
    dxlib.postJSON = postJSON;
})(dxlib);
export default dxlib;
