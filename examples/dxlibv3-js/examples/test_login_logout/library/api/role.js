import * as api from './core.js';

export async function role_create(keys, nameid, name, description) {
    const response = await api.APILogged(keys, "/role/create", {
        "nameid": nameid,
        "name": name,
        "description": description
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function role_read_nameid(keys, nameid) {
    const response = await api.APILogged(keys, "/role/read/nameid", {
        "nameid": nameid
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON["user_management.role"]
}
