import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_organization(keys) {
    let organization_PEGASOL_id = await apiWebadmin.organization_create(keys,
        'PEGASOL',
        null,
        "PARTNER", "" +
        "Jakarta",
        "ACTIVE",
        "LDAP-1",
        "",
        null,
        null
    );
    await apiWebadmin.organization_create(keys,
        'CV Partner A',
        organization_PEGASOL_id,
        "PARTNER", "" +
        "Jakarta",
        "ACTIVE",
        "null",
        "",
        null,
        null
    );
    await apiWebadmin.organization_create(keys,
        'CV Partner B',
        organization_PEGASOL_id,
        "PARTNER", "" +
        "Jakarta",
        "ACTIVE",
        null,
        null,
        null,
        null
    );
}
