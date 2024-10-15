import * as api from './core.js';

export async function announcement_create(keys, image, title, content) {
    const response = await api.APILogged(keys, "/announcement/create", {
        "title": title,
        "content": content,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function announcement_list(keys,
                                        filter_where,
                                        filter_order_by,
                                        filter_key_values,
                                        row_per_page,
                                        page_index,
                                        is_deleted
) {
    const response = await api.APILogged(keys, "/announcement/list", {
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