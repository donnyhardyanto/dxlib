import * as api from './core.js';
import * as self from './self.js'

export async function user_create(keys, organizationId, roleId, loginid, email, fullname, phonenumber, status, password, membership_number) {

    await self.self_prekey(keys);

    const lvPassword = new dxlibv3.LV(password);

    const dataBlockEnvelopeAsHexString = await dxlibv3.packLVPayload(keys.preKeyIndex, keys.edA0PrivateKeyAsBytes, keys.sharedKey1AsBytes, [lvPassword]);

    const response = await api.APILogged(keys, "/user/create", {
        "role_id": roleId,
        "organization_id": organizationId,
        "loginid": loginid,
        "email": email,
        "fullname": fullname,
        "phonenumber": phonenumber,
        "status": status,
        "membership_number": membership_number,
        "password_i": keys.preKeyIndex,
        "password_d": dataBlockEnvelopeAsHexString,
    }, true)

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function create_field_supervisor(keys, user_id) {
    const response = await api.APILogged(keys,
        "/field_supervisor/create",
        {user_id: user_id},
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function create_field_executor(keys, user_id, field_supervisor_id) {
    const response = await api.APILogged(keys,
        "/field_executor/create",
        {user_id: user_id, field_supervisor_id: field_supervisor_id},
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function create_field_executor_location(keys, user_id, location_code) {
    const response = await api.APILogged(keys,
        "/field_executor_location/create",
        {user_id: user_id, location_code: location_code},
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function user_read(keys, id) {
    const response = await api.APILogged(keys, "/user/read", {
        "id": id
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON["user_management.user"]
}