import './libs/tweetnacl/nacl.js';

(function (dxlibv3) {
        'use strict';

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
                    throw new Error('Both keys must be Uint8Arrays');
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
                for (let i = 0; i < lvs.length; i++) {
                    /** @type {LV} */
                    let t = lvs[i]
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
        }

        class AES {
            static async encrypt(key, data) {
                let decodedKey;
                let iv;
                let ciphertextArrayBuffer;
                let ciphertextArray;
                let resultArray;
                try {
                    decodedKey = await window.crypto.subtle.importKey(
                        "raw",
                        key.buffer,
                        "AES-CBC",
                        false,
                        ["encrypt", "decrypt"]
                    );

                    iv = window.crypto.getRandomValues(new Uint8Array(16));
                    ciphertextArrayBuffer = await window.crypto.subtle.encrypt(
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
                    decodedKey = await window.crypto.subtle.importKey(
                        "raw",
                        key.buffer,
                        {name: "AES-CBC"},
                        false,
                        ["encrypt", "decrypt"] //only allow the key to be used for decryption
                    );

                    decryptedArrayBuffer = await window.crypto.subtle.decrypt(
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
            let lvPackedPayload = dxlibv3.LV.combine(arrayOfLvParams);
            let lvPackedPayloadAsBytes = lvPackedPayload.marshalBinary();

            let dataBlock = new dxlibv3.DataBlock(lvPackedPayloadAsBytes);
            dataBlock.PreKey.setValue(preKeyIndex);
            let lvDataBlock = dataBlock.asLV();
            let lvDataBlockAsBytes = lvDataBlock.marshalBinary();

            let encryptedLVDataBlockAsBytes = await dxlibv3.AES.encrypt(encryptKey, lvDataBlockAsBytes)
            let lvEncryptedLVDataBlockAsBytes = new dxlibv3.LV(encryptedLVDataBlockAsBytes)
            let signature = Ed25519.sign(encryptedLVDataBlockAsBytes, edSelfPrivateKey)
            let lvSignature = new dxlibv3.LV(signature)
            let lvDataBlockEnvelope = dxlibv3.LV.combine([lvEncryptedLVDataBlockAsBytes, lvSignature])
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
            //dataBlock = DataBlock.unmarshalBinary(lvDecryptedLVDataBlock.Value);

            let timeStamp = dataBlock.Time.getValueAsString();
            let parsedTimestamp = new Date(timeStamp)


            if (parsedTimestamp.toString() === 'Invalid Date') {
                throw new Error("INVALID_TIMESTAMP_DATA");
            }

            const differenceMS = new Date() - parsedTimestamp
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
                if (isNaN(num)) {
                    throw new Error('Invalid byte value');
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
                bytes[i / 2] = parseInt(s, 16);
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

        dxlibv3.Ed25519 = Ed25519;
        dxlibv3.X25519 = X25519;
        dxlibv3.LV = LV;
        dxlibv3.DataBlock = DataBlock;
        dxlibv3.AES = AES;
        dxlibv3.packLVPayload = packLVPayload;
        dxlibv3.unpackLVPayload = unpackLVPayload;
        dxlibv3.bytesToHex = bytesToHex;
        dxlibv3.hexToBytes = hexToBytes;
    }

)
(typeof module !== 'undefined' && module.exports ? module.exports : (self.dxlibv3 = self.dxlibv3 || {}));
