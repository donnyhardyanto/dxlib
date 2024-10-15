import * as api from './core.js';

export async function area_list(
    keys,
    filter_where,
    filter_order_by,
    filter_key_values,
    row_per_page,
    page_index,
    is_deleted
) {
    const response = await api.APILogged(keys, "/area/list", {
        "filter_where": filter_where,
        "filter_order_by": filter_order_by,
        "filter_key_values": filter_key_values,
        "row_per_page": row_per_page,
        "page_index": page_index,
        "is_deleted": is_deleted,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function location_list(
    keys,
    filter_where,
    filter_order_by,
    filter_key_values,
    row_per_page,
    page_index,
    is_deleted
) {
    const response = await api.APILogged(keys, "/location/list", {
        "filter_where": filter_where,
        "filter_order_by": filter_order_by,
        "filter_key_values": filter_key_values,
        "row_per_page": row_per_page,
        "page_index": page_index,
        "is_deleted": is_deleted,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function customer_ref_list(keys,
                                        filter_where,
                                        filter_order_by,
                                        filter_key_values,
                                        row_per_page,
                                        page_index,
                                        is_deleted
) {
    const response = await api.APILogged(keys, "/customer_ref/list", {
        "filter_where": filter_where,
        "filter_order_by": filter_order_by,
        "filter_key_values": filter_key_values,
        "row_per_page": row_per_page,
        "page_index": page_index,
        "is_deleted": is_deleted,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function global_lookup_list(
    keys,
    filter_where,
    filter_order_by,
    filter_key_values,
    row_per_page,
    page_index,
    is_deleted
) {
    const response = await api.APILogged(keys, "/global_lookup/list", {
        "filter_where": filter_where,
        "filter_order_by": filter_order_by,
        "filter_key_values": filter_key_values,
        "row_per_page": row_per_page,
        "page_index": page_index,
        "is_deleted": is_deleted,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}