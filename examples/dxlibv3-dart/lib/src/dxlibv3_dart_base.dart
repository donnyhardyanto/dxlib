import 'package:cryptography/cryptography.dart' as cryptography;
import 'package:crypto/crypto.dart' as cryptoHash;
import 'package:pinenacl/ed25519.dart';
import 'package:pinenacl/x25519.dart';
import 'package:pinenacl/src/tweetnacl/tweetnacl.dart';
import 'dart:convert';

class KeyPair {
  final Uint8List privateKey;
  final Uint8List publicKey;

  const KeyPair({required this.privateKey, required this.publicKey});
}

class Ed25519 {
  static KeyPair keyPair() {
    var privateKey = Uint8List(TweetNaCl.signingKeyLength);
    var publicKey = Uint8List(TweetNaCl.publicKeyLength);
    int r = TweetNaCl.crypto_sign_keypair(publicKey, privateKey, TweetNaCl.randombytes(TweetNaCl.publicKeyLength));
    if (r != 0) {
      throw Exception('error generating keypair');
    }
    return KeyPair(privateKey: privateKey, publicKey: publicKey);
  }

  static Uint8List sign(msg, selfPrivateKey) {
    var sm = Uint8List(msg.length + TweetNaCl.signatureLength);
    int r = TweetNaCl.crypto_sign(sm, -1, msg, 0, msg.length, selfPrivateKey);
    if (r != 0) {
      throw Exception('error signing message');
    }
    return Uint8List.sublistView(sm, 0, TweetNaCl.signatureLength);
  }

  static bool verify(msg, signature, peerPublicKey) {
    if (signature.length != TweetNaCl.signatureLength) {
      throw Exception('Signature length (${signature.length}) is invalid, expected "${TweetNaCl.signatureLength}"');
    }

    Uint8List sm = Uint8List.fromList(signature + msg);
    Uint8List m = Uint8List(sm.length);
    int r = TweetNaCl.crypto_sign_open(m, -1, sm, 0, sm.length, peerPublicKey);
    return r == 0;
  }
}

class X25519 {
  static keyPair() {
    var privateKey = Uint8List(TweetNaCl.secretKeyLength);
    var publicKey = Uint8List(TweetNaCl.publicKeyLength);
    privateKey = TweetNaCl.randombytes(TweetNaCl.secretKeyLength);
    publicKey = TweetNaCl.crypto_scalarmult_base(publicKey, privateKey);
    return KeyPair(privateKey: privateKey, publicKey: publicKey);
  }

  static Uint8List computeSharedSecret(Uint8List privateAKey, Uint8List publicBKey) {
    var q = Uint8List(TweetNaCl.sharedKeyLength);
    TweetNaCl.crypto_scalarmult(q, privateAKey, publicBKey);
    return q;
  }
}

String bytesToHex(Uint8List bytes) {
  return bytes.map((byte) => byte.toRadixString(16).padLeft(2, '0')).join();
}

Uint8List hexToBytes(String hexString) {
  if (hexString.length % 2 != 0) {
    throw FormatException('Odd number of hex digits');
  }
  var l = hexString.length ~/ 2;
  var result = Uint8List(l);
  for (var i = 0; i < l; i++) {
    var x = int.parse(hexString.substring(i * 2, (i * 2) + 2), radix: 16);
    if (x.isNaN) {
      throw FormatException('Expected hex string');
    }
    result[i] = x;
  }
  return result;
}

bool compareUint8Lists(Uint8List v1, Uint8List v2) {
  if (v1.length != v2.length) {
    return false;
  }
  for (int i = 0; i < v1.length; i++) {
    if (v1[i] != v2[i]) {
      return false;
    }
  }
  return true;
}

class LV {
  late Uint8List value;
  late int length;

  LV(Uint8List aValue) {
    setValue(aValue);
  }

  LV.fromString(String aValue) {
    Uint8List t = utf8.encode(aValue);
    setValue(t);
  }

  LV.empty() {
    setValue(Uint8List(0));
  }

  setValueAsString(String aValue) {
    Uint8List t = utf8.encode(aValue);
    setValue(t);
  }

  String getValueAsString() {
    return utf8.decode(value, allowMalformed: false);
  }

  void setValue(Uint8List aValue) {
    value = aValue;
    length = value.length;
  }

  static LV combine2(LV lv1, LV lv2) {
    Uint8List lv1AsBytes = lv1.marshalBinary(); // Note: Dart's marshalBinary returns Uint8Array.
    Uint8List lv2AsBytes = lv2.marshalBinary();
    Uint8List r = Uint8List.fromList([...lv1AsBytes, ...lv2AsBytes]);
    return LV(r);
  }

  static LV combine(List<LV> lvs) {
    int totalLength = 0;
    List<Uint8List> lvBs = [];
    for (final lv in lvs) {
      Uint8List lvAsBytesArray = lv.marshalBinary();
      lvBs.add(lvAsBytesArray);
      totalLength += lvAsBytesArray.length;
    }

    Uint8List combinedList = Uint8List(totalLength);

    int i = 0;
    for (final lvB in lvBs) {
      int j = i + lvB.length;
      //final Uint8List lvAsBytesArray = lv.marshalBinary();
      combinedList.setRange(i, j, lvB);
      i = j;
    }
    LV r = LV(combinedList);
    return r;
  }

  List<LV> expand() {
    ByteData data = ByteData.view(value.buffer);
    int i = 0, j = 0;
    final List<LV> r = <LV>[];
    while (i < length) {
      int l = data.getInt32(i, Endian.big);
      i += 4;
      j = i + l;
      Uint8List v = value.sublist(i, j);
      LV e = LV(v);
      r.add(e);
      i = j;
    }
    return r;
  }

  Uint8List marshalBinary() {
    int l = length + 4;
    Uint8List r = Uint8List(l);
    ByteData data = ByteData.view(r.buffer);
    data.setInt32(0, length, Endian.big);
    for (int i = 0; i < value.length; ++i) {
      data.setInt8(4 + i, value[i]);
    }
    return r;
  }

  static LV unmarshalBinary(Uint8List v) {
    ByteData data = ByteData.view(v.buffer);
    int l = data.getInt32(0, Endian.big);
    Uint8List t = v.sublist(4, 4 + l);
    LV r = LV(t);
    return r;
  }
}

class DataBlock {
  late LV Time;
  late LV Nonce;
  late LV PreKey;
  late LV Data;
  late LV DataHash;

  DataBlock(Uint8List data) {
    Time = LV.empty();
    Nonce = LV.empty();
    PreKey = LV.empty();
    Data = LV.empty();
    DataHash = LV.empty();
    setTimeNow();
    generateNonce();
    setDataValue(data);
  }

  void setTimeNow() {
    final now = DateTime.now().toUtc().toIso8601String();
    print(now);
    Time.setValueAsString(now);
  }

  void generateNonce() {
    Nonce.setValue(TweetNaCl.randombytes(32));
  }

  void setDataValue(Uint8List data) {
    Data.setValue(data);
    generateDataHash();
  }

  void generateDataHash() {
    cryptoHash.Digest hash = cryptoHash.sha512.convert(Data.value);
    Uint8List msgHash = Uint8List.fromList(hash.bytes);
    DataHash.setValue(msgHash);
  }

  bool checkDataHash() {
    cryptoHash.Digest hash = cryptoHash.sha512.convert(Data.value);
    Uint8List msgHash = Uint8List.fromList(hash.bytes);
    return compareUint8Lists(DataHash.value, msgHash);
  }

  LV asLV() {
    return LV.combine([Time, Nonce, PreKey, Data, DataHash]);
  }

  DataBlock.fromLV(LV aLV) {
    final lvs = aLV.expand();
    Time = lvs[0];
    Nonce = lvs[1];
    PreKey = lvs[2];
    Data = lvs[3];
    DataHash = lvs[4];
  }

/*
  Uint8List marshalBinary() {
    LV t = LV.combine([this.Time, this.Nonce, this.PreKey, this.Data, this.DataHash]);
    return t.value;
  }

  static unmarshalBinary(Uint8List data) {
    int x = 0;
    final lvs = [this.Time, this.Nonce, this.PreKey, this.Data, this.DataHash]
    for (int i = 0; i < lvs.length; i++) {
      lv = .unmarshalBinary(data.slice(x))
      x = x + 4 + lvs[i].Length
    }
  }*/
}

class AES {
  static Future<Uint8List> encrypt(Uint8List key, Uint8List data) async {
    final algorithm = cryptography.AesCbc.with256bits(
      macAlgorithm: cryptography.MacAlgorithm.empty,
    );
    try {
      final iv = TweetNaCl.randombytes(16); // equivalent to crypto.getRandomValues

      cryptography.SecretKey secretKey = cryptography.SecretKey(key);

      cryptography.SecretBox secretBox = await algorithm.encrypt(
        data,
        secretKey: secretKey,
        nonce: iv,
      );

      final resultArray = Uint8List(iv.length + secretBox.cipherText.length);
      resultArray.setRange(0, iv.length, iv);
      resultArray.setRange(iv.length, resultArray.length, secretBox.cipherText);
      return resultArray;
    } catch (err) {
      print(err);
      rethrow;
    }
  }

  static Future<Uint8List> decrypt(Uint8List key, Uint8List encrypted) async {
    final algorithm = cryptography.AesCbc.with256bits(
      macAlgorithm: cryptography.MacAlgorithm.empty,
    );
    try {
      cryptography.SecretKey secretKey = cryptography.SecretKey(key);
      cryptography.SecretBox secretBox = cryptography.SecretBox.fromConcatenation(
        encrypted,
        nonceLength: algorithm.nonceLength,
        //macLength: cryptography.Hmac.sha256().macLength,
        macLength: 0,
        copy: false,
      );

      List<int> decryptedArrayBuffer = await algorithm.decrypt(
        secretBox,
        secretKey: secretKey,
      );

      return Uint8List.fromList(decryptedArrayBuffer);
    } catch (err) {
      print(err);
      rethrow;
    }
  }
}

Future<String> packLVPayload(String preKeyIndex, Uint8List edSelfPrivateKey, Uint8List encryptKey, List<LV> arrayOfLvParams) async {
  LV lvPackedPayload = LV.combine(arrayOfLvParams);
  Uint8List lvPackedPayloadAsBytes = lvPackedPayload.marshalBinary();

  DataBlock dataBlock = DataBlock(lvPackedPayloadAsBytes);
  dataBlock.PreKey.setValueAsString(preKeyIndex);
  LV lvDataBlock = dataBlock.asLV();
  Uint8List lvDataBlockAsBytes = lvDataBlock.marshalBinary();

  Uint8List encryptedLVDataBlockAsBytes = await AES.encrypt(encryptKey, lvDataBlockAsBytes);
  LV lvEncryptedLVDataBlockAsBytes = LV(encryptedLVDataBlockAsBytes);
  Uint8List signature = Ed25519.sign(encryptedLVDataBlockAsBytes, edSelfPrivateKey);
  LV lvSignature = LV(signature);
  LV lvDataBlockEnvelope = LV.combine([lvEncryptedLVDataBlockAsBytes, lvSignature]);
  Uint8List lvDataBlockEnvelopeAsBytes = lvDataBlockEnvelope.marshalBinary();
  return bytesToHex(lvDataBlockEnvelopeAsBytes);
}

const UNPACK_TTL_MS = 5 * 60 * 1000;

Future<List<LV>> unpackLVPayload(String preKeyIndex, Uint8List peerPublicKey, Uint8List decryptKey, String dataAsHexString, {bool skipVerify = false}) async {
  Uint8List dataAsBytes = Uint8List.fromList(hexToBytes(dataAsHexString));

  LV lvData = LV.unmarshalBinary(dataAsBytes);

  List<LV> lvDataElements = lvData.expand();

  if (lvDataElements.isEmpty || lvDataElements.length < 2) {
    throw Exception('INVALID_DATA');
  }

  LV lvEncryptedData = lvDataElements[0];
  LV lvSignature = lvDataElements[1];

  if (!skipVerify) {
    bool valid = Ed25519.verify(lvEncryptedData.value, lvSignature.value, peerPublicKey);
    if (!valid) {
      throw Exception('INVALID_SIGNATURE');
    }
  }

  Uint8List decryptedData = await AES.decrypt(decryptKey, lvEncryptedData.value);

  LV lvDecryptedLVDataBlock = LV.unmarshalBinary(decryptedData);

  DataBlock dataBlock = DataBlock.fromLV(lvDecryptedLVDataBlock);

  String timeStamp = dataBlock.Time.getValueAsString();
  DateTime? parsedTimestamp;
  try {
    parsedTimestamp = DateTime.parse(timeStamp);
  } catch (e) {
    throw Exception("INVALID_TIMESTAMP_DATA");
  }

  final differenceMS = DateTime.now().difference(parsedTimestamp).inMilliseconds;
  if ((differenceMS - UNPACK_TTL_MS) > 0) {
    throw Exception("TIME_EXPIRED");
  }

  String dataBlockPreKeyIndex = dataBlock.PreKey.getValueAsString();
  if (dataBlockPreKeyIndex != preKeyIndex) {
    throw Exception('INVALID_PREKEY');
  }

  if (!dataBlock.checkDataHash()) {
    throw Exception('INVALID_DATA_HASH');
  }

  Uint8List lvCombinedPayloadAsBytes = dataBlock.Data.value;

  LV lvCombinedPayload = LV.unmarshalBinary(lvCombinedPayloadAsBytes);
  List<LV> lvPtrDataPayload = lvCombinedPayload.expand();

  return lvPtrDataPayload;
}
