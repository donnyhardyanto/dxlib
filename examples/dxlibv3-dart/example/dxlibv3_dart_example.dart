import 'dart:convert';
import 'dart:typed_data';

import 'package:dxlibv3_dart/dxlibv3_dart.dart';
import 'package:http/http.dart' as http;

void testCrypto() {
  // Generate key pairs for Alice and Bob
  print('Generating key pairs...');
  KeyPair a0 = Ed25519.keyPair();
  KeyPair b0 = Ed25519.keyPair();
  KeyPair a1 = X25519.keyPair();
  KeyPair b1 = X25519.keyPair();

  final aliceKeyPair = a1.privateKey;
  final bobKeyPair = b1.privateKey;

  // Extract public keys
  final alicePublicKey = a1.publicKey;
  final bobPublicKey = b1.publicKey;

  print('Alice sign private key: ${bytesToHex(a0.privateKey)}');
  print('Bob sign private key: ${bytesToHex(b0.privateKey)}');
  print('Alice sign public key: ${bytesToHex(a0.publicKey)}');
  print('Bob sign public key: ${bytesToHex(b0.publicKey)}');

  print('Alice public key: ${bytesToHex(alicePublicKey)}');
  print('Bob public key:    ${bytesToHex(bobPublicKey)}');
  print('Alice private key: ${bytesToHex(aliceKeyPair)}');
  print('Bob private key:   ${bytesToHex(bobKeyPair)}');

  // Compute shared secrets
  print('\nComputing shared secrets...');
  Uint8List aliceSharedSecret = X25519.computeSharedSecret(aliceKeyPair, bobPublicKey);
  Uint8List bobSharedSecret = X25519.computeSharedSecret(bobKeyPair, alicePublicKey);

  String aliceSharedSecretAsHexString = bytesToHex(aliceSharedSecret);
  String bobSharedSecretAsHexString = bytesToHex(bobSharedSecret);

  print('Alice\'s shared secret: $aliceSharedSecretAsHexString');
  print('Bob\'s shared secret: $bobSharedSecretAsHexString');

  // Verify that the shared secrets are identical
  print('\nVerifying shared secrets...');
  if (aliceSharedSecretAsHexString == bobSharedSecretAsHexString) {
    print('Success: Alice and Bob have computed the same shared secret.');
  } else {
    print('Error: The computed shared secrets do not match.');
  }
}

String sessionKey = "";
const APISystemProtocol = 'http://';
const APISystemAddress = "0.0.0.0:15001";

Future<void> login() async {
  final ed25519KeyPair = Ed25519.keyPair();
  final edA0PublicKeyAsBytes = ed25519KeyPair.publicKey;
  final edA0PrivateKeyAsBytes = ed25519KeyPair.privateKey;

  final x25519KeyPair1 = X25519.keyPair();
  final ecdhA1PublicKeyAsBytes = x25519KeyPair1.publicKey;
  final ecdhA1PrivateKeyAsBytes = x25519KeyPair1.privateKey;

  final x25519KeyPair2 = X25519.keyPair();
  final ecdhA2PublicKeyAsBytes = x25519KeyPair2.publicKey;
  final ecdhA2PrivateKeyAsBytes = x25519KeyPair2.privateKey;

  // Convert keys to string
  final edA0PublicKeyAsHexString = bytesToHex(edA0PublicKeyAsBytes);
  final ecdhA1PublicKeyAsHexString = bytesToHex(ecdhA1PublicKeyAsBytes);
  final ecdhA2PublicKeyAsHexString = bytesToHex(ecdhA2PublicKeyAsBytes);

  final preLoginResponse = await http.post(
    Uri.parse('$APISystemProtocol$APISystemAddress/self/prekey'),
    headers: {'Content-Type': 'application/json'},
    body: jsonEncode({
      'a0': edA0PublicKeyAsHexString,
      'a1': ecdhA1PublicKeyAsHexString,
      'a2': ecdhA2PublicKeyAsHexString,
    }),
  );

  if (preLoginResponse.statusCode != 200) {
    throw Exception("Status code is not 200 but ${preLoginResponse.statusCode}");
  }

  final preLoginResponseDataAsJSON = jsonDecode(preLoginResponse.body);

  final index = preLoginResponseDataAsJSON['i'];
  final edB0PublicKeyAsHexString = preLoginResponseDataAsJSON['b0'];
  final ecdhB1PublicKeyAsHexString = preLoginResponseDataAsJSON['b1'];
  final ecdhB2PublicKeyAsHexString = preLoginResponseDataAsJSON['b2'];

  final edB0PublicKeyAsBytes = hexToBytes(edB0PublicKeyAsHexString);
  final ecdhB1PublicKeyAsBytes = hexToBytes(ecdhB1PublicKeyAsHexString);
  final ecdhB2PublicKeyAsBytes = hexToBytes(ecdhB2PublicKeyAsHexString);

  final sharedKey1AsBytes = X25519.computeSharedSecret(ecdhA1PrivateKeyAsBytes, ecdhB1PublicKeyAsBytes);
  final sharedKey2AsBytes = X25519.computeSharedSecret(ecdhA2PrivateKeyAsBytes, ecdhB2PublicKeyAsBytes);

  const userLogin = 'superadmin';
  const password = 'bebekangsa';

  final lvUserLogin = LV.fromString(userLogin);
  final lvPassword = LV.fromString(password);

  final dataBlockEnvelopeAsHexString = await packLVPayload(index, edA0PrivateKeyAsBytes, sharedKey1AsBytes, [lvUserLogin, lvPassword]);

  final loginResponse = await http.post(
    Uri.parse('$APISystemProtocol$APISystemAddress/self/login'),
    headers: {'Content-Type': 'application/json'},
    body: jsonEncode({
      'i': index,
      'd': dataBlockEnvelopeAsHexString,
    }),
  );

  if (loginResponse.statusCode != 200) {
    throw Exception("Status code is not 200 but ${loginResponse.statusCode}");
  }

  final loginResponseDataAsJSON = jsonDecode(loginResponse.body);
  final dataBlockEnvelopeAsHexString2 = loginResponseDataAsJSON['d'];

  List<LV> lvPayloadElements = await unpackLVPayload(index, edB0PublicKeyAsBytes, sharedKey2AsBytes, dataBlockEnvelopeAsHexString2);

  LV lvSessionObject = lvPayloadElements[0];

  String sessionObjectAsString = lvSessionObject.getValueAsString();
  print(sessionObjectAsString);

  var sessionObject = jsonDecode(sessionObjectAsString);
  print(sessionObject);

  sessionKey = sessionObject['session_key'];

  if (sessionKey == "") {
    throw Exception("Invalid resulted session key");
  }

  print('$sessionKey logged in');
}

Future<void> logout() async {
  try {
    final logoutResponse = await http.post(
      Uri.parse('$APISystemProtocol$APISystemAddress/self/logout'),
      headers: {'Content-Type': 'application/json', 'Authorization': 'Bearer $sessionKey'},
      body: jsonEncode({}),
    );

    if (logoutResponse.statusCode != 200) {
      throw Exception("Status code is not 200 but ${logoutResponse.statusCode}");
    }

    print('$sessionKey logged out');
  } catch (e) {
    // Handle any errors that occurred during the logout process
    print('Error during logout: $e');
    rethrow; // Re-throw the exception if you want calling code to handle it
  }
}

Future<void> main(List<String> arguments) async {
  testCrypto();
  await login();
  await logout();
}
