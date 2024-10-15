import * as api from './core.js';

export async function external_system_create(keys, nameid, type, configuration) {
    const response = await api.APILogged(keys, "/external_system/create", {
        "nameid": nameid,
        "type": type,
        "configuration": configuration
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

