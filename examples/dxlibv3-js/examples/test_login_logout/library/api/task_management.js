import * as api from './core.js';

export async function customer_add(keys, {
                                       registration_number,
                                       customer_number,
                                       fullname,
                                       status,
                                       email,
                                       phonenumber,
                                       korespondensi_media,
                                       identity_type,
                                       identity_number,
                                       npwp,
                                       customer_segment_code,
                                       customer_type_code,
                                       jenis_anggaran,
                                       rs_customer_sector_code,
                                       sales_area_code,
                                       latitude,
                                       longitude,
                                       address_street,
                                       address_rt,
                                       address_rw,
                                       address_kelurahan_location_code,
                                       address_kecamatan_location_code,
                                       address_kabupaten_location_code,
                                       address_province_location_code,
                                       address_postal_code,
                                       register_at,
                                       jenis_bangunan,
                                       program_pelanggan,
                                       kategory_pelanggan,
                                       skema_pembayaran,
                                       kategory_wilayah,
                                   }) {
    const response = await api.APILogged(keys,
        "/customer/create",
        {
            registration_number: registration_number,
            customer_number: customer_number,
            fullname: fullname,
            status: status,
            email: email,
            phonenumber: phonenumber,
            korespondensi_media: korespondensi_media,
            identity_type: identity_type,
            identity_number: identity_number,
            npwp: npwp,
            customer_segment_code: customer_segment_code,
            customer_type_code: customer_type_code,
            jenis_anggaran: jenis_anggaran,
            rs_customer_sector_code: rs_customer_sector_code,
            sales_area_code: sales_area_code,
            latitude: latitude,
            longitude: longitude,
            address_street: address_street,
            address_rt: address_rt,
            address_rw: address_rw,
            address_kelurahan_location_code: address_kelurahan_location_code,
            address_kecamatan_location_code: address_kecamatan_location_code,
            address_kabupaten_location_code: address_kabupaten_location_code,
            address_province_location_code: address_province_location_code,
            address_postal_code: address_postal_code,
            register_at: register_at,
            jenis_bangunan: jenis_bangunan,
            program_pelanggan: program_pelanggan,
            kategory_pelanggan: kategory_pelanggan,
            skema_pembayaran: skema_pembayaran,
            kategory_wilayah: kategory_wilayah,
        },
        true
    );
    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function customer_edit(keys,
                                    id,
    newData
) {
    const response = await api.APILogged(keys,
        "/customer/edit",
        {
            id: id,
            new: newData,
        },
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON);
    return responseDataAsJSON;
}

export async function task_add(keys, {code, task_type_id, status, customer_id, data1, data2}) {
    const response = await api.APILogged(keys,
        "/task/create",
        {
            code: code,
            task_type_id: task_type_id,
            status: status,
            customer_id: customer_id,
            data1: data1,
            data2: data2,
        },
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function task_edit(keys, id, newData) {
    const response = await api.APILogged(keys,
        "/task/edit",
        {
            id: id,
            new: newData,
        },
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON);
    return responseDataAsJSON;
}

export async function sub_task_add(keys, {task_id, sub_task_type_id, code, sub_task_status, field_executor_user_id, estimated_start_date, estimated_end_date}) {
    const response = await api.APILogged(keys,
        "/sub_task/create",
        {
            task_id: task_id,
            sub_task_type_id: sub_task_type_id,
            code: code,
            sub_task_status: sub_task_status,
            field_executor_user_id: field_executor_user_id,
            estimated_start_date: estimated_start_date,
            estimated_end_date: estimated_end_date,
        },
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON['id']
}

export async function sub_task_edit(keys, id, newData) {
    const response = await api.APILogged(keys,
        "/sub_task/edit",
        {
            id: id,
            new: newData,
        },
        true
    );

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON);
    return responseDataAsJSON;
}

export async function sub_task_read(keys, id) {
    const response = await api.APILogged(keys, "/sub_task/read", {
        "id": id
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON["task_management.sub_task"]
}

export async function sub_task_pick(keys, sub_task_id) {
    const response = await api.APILogged(keys, "/sub_task/pick", {
        "sub_task_id": sub_task_id
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_schedule(keys, sub_task_id, start_date, end_date) {
    const response = await api.APILogged(keys, "/sub_task/schedule", {
        "sub_task_id": sub_task_id,
        "start_date": start_date,
        "end_date": end_date
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_working_start(keys, sub_task_id, at) {
    const response = await api.APILogged(keys, "/sub_task/working_start", {
        "sub_task_id": sub_task_id,
        "at": at,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_working_finish(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/working_finish", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_verify_start(keys, sub_task_id, at) {
    const response = await api.APILogged(keys, "/sub_task/verify_start", {
        "sub_task_id": sub_task_id,
        "at": at,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_verify_success(keys, sub_task_id, at) {
    const response = await api.APILogged(keys, "/sub_task/verify_success", {
        "sub_task_id": sub_task_id,
        "at": at,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_verify_fail(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/verify_fail", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_fixing_start(keys, sub_task_id, at) {
    const response = await api.APILogged(keys, "/sub_task/fixing_start", {
        "sub_task_id": sub_task_id,
        "at": at,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_fixing_finish(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/fixing_finish", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_pause(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/pause", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_resume(keys, sub_task_id, at) {
    const response = await api.APILogged(keys, "/sub_task/resume", {
        "sub_task_id": sub_task_id,
        "at": at,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_cancel_by_field_executor(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/cancel_by_field_executor", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}

export async function sub_task_cancel_by_customer(keys, sub_task_id, at, report) {
    const response = await api.APILogged(keys, "/sub_task/cancel_by_customer", {
        "sub_task_id": sub_task_id,
        "at": at,
        "report": report,
    }, true);

    const responseDataAsJSON = await response.json();
    console.log(responseDataAsJSON)
    return responseDataAsJSON
}