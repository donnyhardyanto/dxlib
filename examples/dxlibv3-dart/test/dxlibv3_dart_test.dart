import 'package:test/test.dart';

import '../example/dxlibv3_dart_example.dart';

void main() {
  group('Test Login Logout', () {
    test('Login', () async {
      await login();
      await logout();
    });
  });
}
