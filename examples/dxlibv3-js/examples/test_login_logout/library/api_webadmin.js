import { announcement_create, announcement_list } from "./api/announcement.js";
import { APILogged, APIPublic } from "./api/core.js";
import { external_system_create } from "./api/external_system.js";
import { area_list, customer_ref_list, global_lookup_list, location_list } from "./api/master-data.js";
import { organization_create, organization_read_name } from "./api/organization.js";
import { role_create, role_read_nameid } from "./api/role.js";
import { self_detail, self_login, self_logout, self_password_change, self_prekey } from "./api/self.js";
import { customer_add, customer_edit, sub_task_add, sub_task_edit, task_add, task_edit, sub_task_read, sub_task_pick } from "./api/task_management.js";
import { user_create, create_field_supervisor, create_field_executor, create_field_executor_location, user_read } from "./api/user.js";

export let keys = {
    edA0PrivateKeyAsBytes: null,
    ecdhA1PrivateKeyAsBytes: null,
    ecdhA2PrivateKeyAsBytes: null,
    edB0PublicKeyAsBytes: null,
    sharedKey1AsBytes: null,
    sharedKey2AsBytes: null,
    preKeyIndex: null,
    sessionKey: null,
    userId: null,
    APIAddress:  null
};

export {
    announcement_create,
    announcement_list, APILogged, APIPublic, area_list, customer_add,
    customer_edit, customer_ref_list, external_system_create, global_lookup_list, location_list, organization_create,
    organization_read_name, role_create,
    role_read_nameid, self_detail, self_login,
    self_logout, self_password_change, self_prekey, sub_task_add,
    sub_task_edit, task_add, sub_task_read,sub_task_pick,
    task_edit, user_create, create_field_supervisor, create_field_executor, create_field_executor_location, user_read,
};

