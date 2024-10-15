import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_external_system(keys) {
    await apiWebadmin.external_system_create(keys,'LDAP-1', 'LDAP', `
        {
            "host": "ldap://127.0.0.1:389", 
            "base_dn": "dc=areta,dc=com", 
            "bind_dn": "cn=admin,dc=areta,dc=com", 
            "bind_password": "adminAreta"
        }
    `);
}
