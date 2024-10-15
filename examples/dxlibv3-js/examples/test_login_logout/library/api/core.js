
export function assertResponse(response) {
    let statusCode = response.status;
    if (statusCode !== 200) {
        alert(`${response.url}: Status code is ${statusCode.toString()}`);
        throw new Error("Execution halted");
    }
}

export async function APIPublic(keys, url, jsonRequestData, asserted) {
    let bodyAsString = "";
    if (jsonRequestData !== null) {
        bodyAsString = JSON.stringify(jsonRequestData);
    }
    let response = await fetch(keys.APIAddress + url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: bodyAsString,
    });
    if (asserted) {
        assertResponse(response);
    }
    return response;
}

export async function APILogged(keys, url, jsonRequestData, asserted) {
    let bodyAsString = "";
    if (jsonRequestData !== null) {
        bodyAsString = JSON.stringify(jsonRequestData);
    }
    let response = await fetch(keys.APIAddress + url, {
        method: 'POST',
        headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${keys.sessionKey}`,
        },
        body: bodyAsString,
    });
    if (asserted) {
        assertResponse(response);
    }
    return response;
}
