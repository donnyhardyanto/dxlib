import * as api from './core.js';

export async function organization_create(keys, name, parent_id, type, address, status, auth_source1, attribute1, auth_source2, attribute2) {
    const response = await api.APILogged(keys, "/organization/create", {
        name: name,
        parent_id: parent_id,
        type: type,
        address: address,
        status: status,
        auth_source1: auth_source1,
        attribute1: attribute1,
        auth_source2: auth_source2,
        attribute2: attribute2,
    }, true)

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function organization_read_name(keys, name) {
    const response = await api.APILogged(keys, "/organization/read/name", {
        "name": name
    }, true)

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON["user_management.organization"]
}
