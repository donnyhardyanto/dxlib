import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_master_data(keys) {
    let l
    l = await apiWebadmin.area_list(keys,
        "", "", {}, 0, 0, false
    )
    console.log("area_list", l)

    l = await apiWebadmin.location_list(keys,
        "", "", {}, 0, 0, false
    )
    console.log("location_list", l)

    l = await apiWebadmin.customer_ref_list(keys,
        "", "", {}, 0, 0, false
    )
    console.log("customer_ref_list", l)

    l = await apiWebadmin.global_lookup_list(keys,
        "", "", {}, 0, 0, false
    )
    console.log("global_lookup_list", l)
}
