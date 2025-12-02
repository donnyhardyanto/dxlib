
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
    dxlib.bytesToHex = bytesToHex;
    dxlib.hexToBytes = hexToBytes;
    dxlib.assertResponse = assertResponse;
    dxlib.api = api;
    dxlib.apiUpload = apiUpload;
    dxlib.b64ToHex = b64ToHex;
    dxlib.postJSON = postJSON;
}) (dxlib);

if (typeof module !== 'undefined' && module.exports) {
    module.exports = dxlib;
} else {
    globalThis.dxlib = globalThis.dxlib || dxlib;
}

export default dxlib;
