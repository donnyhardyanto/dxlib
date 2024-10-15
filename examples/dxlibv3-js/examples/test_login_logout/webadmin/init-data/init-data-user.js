import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_user(keys) {
    let role_PELAKSANA_LAPANGAN = await apiWebadmin.role_read_nameid(keys, "PELAKSANA_LAPANGAN");
    let role_PENGAWAS_LAPANGAN = await apiWebadmin.role_read_nameid(keys, "PENGAWAS_LAPANGAN");
    let organization_PEGASOL = await apiWebadmin.organization_read_name(keys, "PEGASOL");
    let organization_CVPartnerA = await apiWebadmin.organization_read_name(keys, "CV Partner A");
    let organization_CVPartnerB = await apiWebadmin.organization_read_name(keys, "CV Partner B");

    let uplID = await apiWebadmin.user_create(keys,
        organization_CVPartnerA.id,
        role_PELAKSANA_LAPANGAN.id,
        "+628111088120",
        "hardyanto.donny@gmail.com",
        "Donny Hardyanto",
        "+628111088120",
        "ACTIVE",
        "bebekangsa",
        "3000000001"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerA.id,
        role_PELAKSANA_LAPANGAN.id,
        "+6287747393058",
        "fajar.riswandi05@gmail.com",
        "Fajar Riswandi",
        "+6287747393058",
        "ACTIVE",
        "bebekangsa",
        "3000000002"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000111",
        "erwin.gunardi@gmail.com",
        "Erwin Gunardi",
        "+62877000111",
        "ACTIVE",
        "bebekangsa",
        "3000001499"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000112",
        "rob.wilson1987@yopmail.com",
        "Robert Wilson",
        "+62877000112",
        "ACTIVE",
        "bebekangsa",
        "3002300138"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000113",
        "lmiller33@yopmail.com",
        "Lisa Miller",
        "+62877000113",
        "ACTIVE",
        "bebekangsa",
        "3004300129"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000114",
        "matt.moore93@yopmail.com",
        "Matthew Moore",
        "+62877000114",
        "ACTIVE",
        "bebekangsa",
        "3005400110"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerA.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000115",
        "emily.johnson@yopmail.com",
        "Emily Johnson",
        "+62877000115",
        "ACTIVE",
        "bebekangsa",
        "3000003105"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerA.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000116",
        "david.williams22@yopmail.com",
        "David Williams",
        "+62877000116",
        "ACTIVE",
        "bebekangsa",
        "3000400096"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerA.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000117",
        "sarah.davis7@yopmail.com",
        "Sarah Davis",
        "+62877000117",
        "ACTIVE",
        "bebekangsa",
        "3000500087"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000118",
        "john.doe@yopmail.com",
        "John Doe",
        "+62877000118",
        "ACTIVE",
        "bebekangsa",
        "3003000072"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "62877000119",
        "jsmith90@yopmail.com",
        "Jane Smith",
        "+62877000119",
        "ACTIVE",
        "bebekangsa",
        "3020000063"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000120",
        "mbrown2024@yopmail.com",
        "Michael Brown",
        "+62877000120",
        "ACTIVE",
        "bebekangsa",
        "3010000054"
    );

    await apiWebadmin.user_create(keys,
        organization_CVPartnerB.id,
        role_PELAKSANA_LAPANGAN.id,
        "+62877000121",
        "dian.pisesa@gmail.com",
        "Dian Pisesa",
        "+62877000121",
        "ACTIVE",
        "bebekangsa",
        "3000000042"
    );

    await apiWebadmin.user_create(keys,
        organization_PEGASOL.id,
        role_PENGAWAS_LAPANGAN.id,
        "+62877000122",
        "budi_aja@gmail.com",
        "Budi A Raharja",
        "+62877000122",
        "ACTIVE",
        "bebekangsa",
        "3000000032"
    );

    await apiWebadmin.user_create(keys,
        organization_PEGASOL.id,
        role_PENGAWAS_LAPANGAN.id,
        "+62877000123",
        "cendi.cendi@gmail.com",
        "Cendi Ruscendi",
        "+62877000123",
        "ACTIVE",
        "bebekangsa",
        "3000000022"
    );

    let uspID = await apiWebadmin.user_create(keys,
        organization_PEGASOL.id,
        role_PENGAWAS_LAPANGAN.id,
        "+62877000124",
        "rianriansaputra44@gmail.com",
        "Arian Saputra",
        "+62877000124",
        "ACTIVE",
        "bebekangsa",
        "3000000012"
    );

    let spID = await apiWebadmin.create_field_supervisor(keys, uspID)
    await apiWebadmin.create_field_executor(keys, uplID, spID)
    await apiWebadmin.create_field_executor_location(keys, uplID, '11.09.07.2017')
}

