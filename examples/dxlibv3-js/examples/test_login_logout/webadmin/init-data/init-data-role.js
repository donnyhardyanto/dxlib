import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_role(keys) {
    await apiWebadmin.role_create(keys,"PELAKSANA_LAPANGAN", "Pelaksana Lapangan", "Pelaksana lapangan ");
    await apiWebadmin.role_create(keys,"PENGAWAS_LAPANGAN", "Pengawas Lapangan", "Pengawas lapangan");
    await apiWebadmin.role_create(keys,"OM", "Operation Manager", "Operation Manager (Site Coordinator/Constructor)");
    await apiWebadmin.role_create(keys,"ASA", "Admin/Sales Area", "Admin/Sales Area dari SOR");
    await apiWebadmin.role_create(keys,"CGP", "Admin Partner (GCP)", "Admin Partner (GCP)");
}
