import * as apiMobile from "./library/api_mobile.js";
import * as apiWebadmin from "./library/api_webadmin.js";
import * as test_task_management from "./mobile/test/task_management.js";
import { init_data_announcement } from "./webadmin/init-data/init-data-announcement.js";
import { init_data_external_system } from "./webadmin/init-data/init-data-external_system.js";
import { init_data_master_data } from "./webadmin/init-data/init-data-master-data.js";
import { init_data_organization } from "./webadmin/init-data/init-data-organization.js";
import { init_data_role } from "./webadmin/init-data/init-data-role.js";
import { init_data_task_management } from "./webadmin/init-data/init-data-task_management.js";
import { init_data_user } from "./webadmin/init-data/init-data-user.js";

let serverOptions = [
  {
    value: 0,
    label: "http://127.0.0.1",
    addresses: {
      webadmin: "http://127.0.0.1:15000",
      mobile: "http://127.0.0.1:15001",
    },
  },
  {
    value: 1,
    label: "http://157.245.195.5",
    addresses: {
      webadmin: "http:/157.245.195.5:15000",
      mobile: "http://157.245.195.5:15001",
    },
  },
  {
    value: 2,
    label: "https://apps-docker-*.pgn.aretaamany.com",
    addresses: {
      webadmin: "https://apps-docker-webadmin.pgn.aretaamany.com",
      mobile: "https://apps-docker-mobile.pgn.aretaamany.com",
    },
  },
];

async function input_select_api_server_address_onchange() {
  let i = document.getElementById("input_select_api_server_address").value;
  apiWebadmin.keys.APIAddress = serverOptions[i].addresses.webadmin;
  apiMobile.keys.APIAddress = serverOptions[i].addresses.mobile;
}

async function button_webadmin_login_onclick() {
  let loginid = document.getElementById("input_text_webadmin_loginid").value;
  let password = document.getElementById("input_text_webadmin_password").value;
  await apiWebadmin.self_login(apiWebadmin.keys, loginid, password);
}

async function button_webadmin_logout_onclick() {
  return apiWebadmin.self_logout(apiWebadmin.keys);
}

async function button_webadmin_init_data_onclick() {
  await init_data_external_system(apiWebadmin.keys);
  await init_data_organization(apiWebadmin.keys);
  await init_data_role(apiWebadmin.keys);
  await init_data_user(apiWebadmin.keys);
  await init_data_announcement(apiWebadmin.keys);
  await init_data_task_management(apiWebadmin.keys);
}

async function button_webadmin_read_data_onclick() {
  await init_data_master_data(apiWebadmin.keys);
}

async function button_webadmin_self_detail_onclick() {
  await apiWebadmin.self_detail(apiWebadmin.keys);
}

async function button_webadmin_change_password_onclick() {
  let new_password = document.getElementById("input_text_webadmin_new_password").value;
  let old_password = document.getElementById("input_text_webadmin_old_password").value;
  await apiWebadmin.self_password_change(apiWebadmin.keys, new_password, old_password);
}

async function button_mobile_login_onclick() {
  let loginid = document.getElementById("input_text_mobile_loginid").value;
  let password = document.getElementById("input_text_mobile_password").value;
  await apiMobile.self_login(apiMobile.keys, loginid, password);
}

async function button_mobile_login_supervisor_onclick() {
  let loginid = document.getElementById("input_text_mobile_loginid_supervisor").value;
  let password = document.getElementById("input_text_mobile_password_supervisor").value;
  await apiMobile.self_login(apiMobile.keys, loginid, password);
}

function get_sub_task_id(type_id) {
  switch (type_id) {
    case "flow_executor":
      return parseInt(document.getElementById("executer_sub_task_id").value);
    case "flow_supervisor":
      return parseInt(document.getElementById("supervisor_sub_task_id").value);
    case "flow_automation":
      return parseInt(document.getElementById("auto_sub_task_id").value);
    case "flow_automation_sk":
      return 1;
    case "flow_automation_sr":
      return 2;
    case "flow_automation_mi":
      return 3;
    case "flow_automation_gi":
      return 4;
    default:
      return 0;
  }
}

async function button_mobile_s1_sub_task_pick_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.subtask_pick(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_schedule_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_schedule(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_working_start_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_working_start(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_cancel_by_field_executor_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_cancel_by_field_executor(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_cancel_by_customer_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_cancel_by_customer(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_pause_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_pause(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_resume_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_resume(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_working_finish_onclick(type_id, report) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_working_finish(apiMobile.keys, sub_task_id, report);
}

async function button_mobile_s1_sub_task_verify_start_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_verify_start(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_verify_fail_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_verify_fail(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_fixing_start_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_fixing_start(apiMobile.keys, sub_task_id);
}

async function button_mobile_s1_sub_task_fixing_finish_onclick(type_id, report) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_fixing_finish(apiMobile.keys, sub_task_id, report);
}

async function button_mobile_s1_sub_task_verify_success_onclick(type_id) {
  let sub_task_id = get_sub_task_id(type_id);
  await test_task_management.sub_task_verify_success(apiMobile.keys, sub_task_id);
}

async function button_mobile_logout_onclick() {
  return apiMobile.self_logout(apiMobile.keys);
}

async function button_mobile_sub_task_flow_sk_onclick() {
  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_pick_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_schedule_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_working_start_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_pause_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_resume_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_working_finish_onclick("flow_automation_sk", {
    sk: {
      pipe_length: 15,
      calculated_extra_pipe_length: 5,
      test_start_time: "11:04",
      test_end_time: "11:30",
      calculated_test_duration_minute: 26,
      test_pressure: 10.1,
      branch_pipe_availability: true,
      gas_appliance: [
        {
          gas_appliance_id: 1,
          quantity: 2,
        },
        {
          gas_appliance_id: 2,
          quantity: 3,
        },
      ],
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_verify_fail_onclick("flow_automation_sk");

  await button_mobile_logout_onclick();

  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_fixing_start_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_fixing_finish_onclick("flow_automation_sk", {
    sk: {
      pipe_length: 15,
      calculated_extra_pipe_length: 5,
      test_start_time: "11:04",
      test_end_time: "11:30",
      calculated_test_duration_minute: 26,
      test_pressure: 10.1,
      branch_pipe_availability: true,
      gas_appliance: [
        {
          gas_appliance_id: 1,
          quantity: 2,
        },
        {
          gas_appliance_id: 2,
          quantity: 3,
        },
      ],
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_sk");
  await button_mobile_s1_sub_task_verify_success_onclick("flow_automation_sk");

  await button_mobile_logout_onclick();
}

async function button_mobile_sub_task_flow_sr_onclick() {
  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_pick_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_schedule_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_working_start_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_pause_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_resume_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_working_finish_onclick("flow_automation_sr", {
    sr: {
      tapping_saddle_id: 1,
      test_start_time: "11:04",
      test_end_time: "11:30",
      calculated_test_duration_minute: 26,
      test_pressure: 10.1,
      branch_pipe_availability: true,
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_verify_fail_onclick("flow_automation_sr");

  await button_mobile_logout_onclick();

  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_fixing_start_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_fixing_finish_onclick("flow_automation_sr", {
    sr: {
      tapping_saddle_id: 1,
      test_start_time: "11:04",
      test_end_time: "11:30",
      calculated_test_duration_minute: 26,
      test_pressure: 10.1,
      branch_pipe_availability: true,
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_sr");
  await button_mobile_s1_sub_task_verify_success_onclick("flow_automation_sr");

  await button_mobile_logout_onclick();
}

async function button_mobile_sub_task_flow_mi_onclick() {
  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_pick_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_schedule_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_working_start_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_pause_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_resume_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_working_finish_onclick("flow_automation_mi", {
    meter_installation: {
      meter_id: 1,
      meter_brand: "RUCIKA",
      sn_meter: "SNMTR0001",
      g_size_id: 1,
      qmin: 0.02,
      qmax: 2.5,
      pmax: 0.5,
      start_calibration_month: 10,
      start_calibration_year: 2024,
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_verify_fail_onclick("flow_automation_mi");

  await button_mobile_logout_onclick();

  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_fixing_start_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_fixing_finish_onclick("flow_automation_mi", {
    meter_installation: {
      meter_id: 1,
      meter_brand: "RUCIKA",
      sn_meter: "SNMTR0001",
      g_size_id: 1,
      qmin: 0.02,
      qmax: 2.5,
      pmax: 0.5,
      start_calibration_month: 10,
      start_calibration_year: 2024,
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_mi");
  await button_mobile_s1_sub_task_verify_success_onclick("flow_automation_mi");

  await button_mobile_logout_onclick();
}

async function button_mobile_sub_task_flow_gi_onclick() {
  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_pick_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_schedule_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_working_start_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_pause_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_resume_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_working_finish_onclick("flow_automation_gi", {
    gas_in: {
      meter_id: 1,
      meter_brand: "RUCIKA",
      sn_meter: "SNMTR0001",
      g_size_id: 1,
      pmax: 0.5,
      stand_meter_start_number: 0,
      pressure_start: 20,
      temperature_start: 50,
      regulator_brand: "RUCIKA",
      regulator_size_inch: 2,
      meter_location_longitude: 110.20364,
      meter_location_latitude: -7.60573,
      gas_appliance: [
        {
          gas_appliance_id: 1,
          quantity: 2,
        },
        {
          gas_appliance_id: 2,
          quantity: 3,
        },
      ],
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_verify_fail_onclick("flow_automation_gi");

  await button_mobile_logout_onclick();

  await button_mobile_login_onclick();

  await button_mobile_s1_sub_task_fixing_start_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_fixing_finish_onclick("flow_automation_gi", {
    gas_in: {
      meter_id: 1,
      meter_brand: "RUCIKA",
      sn_meter: "SNMTR0001",
      g_size_id: 1,
      pmax: 0.5,
      stand_meter_start_number: 0,
      pressure_start: 20,
      temperature_start: 50,
      regulator_brand: "RUCIKA",
      regulator_size_inch: 2,
      meter_location_longitude: 110.20364,
      meter_location_latitude: -7.60573,
      gas_appliance: [
        {
          gas_appliance_id: 1,
          quantity: 2,
        },
        {
          gas_appliance_id: 2,
          quantity: 3,
        },
      ],
    },
  });

  await button_mobile_logout_onclick();

  await button_mobile_login_supervisor_onclick();

  await button_mobile_s1_sub_task_verify_start_onclick("flow_automation_gi");
  await button_mobile_s1_sub_task_verify_success_onclick("flow_automation_gi");

  await button_mobile_logout_onclick();
}

// Main function
async function main() {
  const selectElement = document.getElementById("input_select_api_server_address");
  serverOptions.forEach((option) => {
    const opt = document.createElement("option");
    opt.value = option.value;
    opt.textContent = option.label;
    selectElement.appendChild(opt);
  });
  selectElement.value = 0;
  await input_select_api_server_address_onchange();
}

window.input_select_api_server_address_onchange = input_select_api_server_address_onchange;
window.button_webadmin_login_onclick = button_webadmin_login_onclick;
window.button_webadmin_logout_onclick = button_webadmin_logout_onclick;
window.button_webadmin_init_data_onclick = button_webadmin_init_data_onclick;
window.button_webadmin_self_detail_onclick = button_webadmin_self_detail_onclick;
window.button_webadmin_change_password_onclick = button_webadmin_change_password_onclick;
window.button_webadmin_read_data_onclick = button_webadmin_read_data_onclick;
window.button_mobile_login_onclick = button_mobile_login_onclick;
window.button_mobile_logout_onclick = button_mobile_logout_onclick;
window.button_mobile_s1_sub_task_pick_onclick = button_mobile_s1_sub_task_pick_onclick;
window.button_mobile_s1_sub_task_schedule_onclick = button_mobile_s1_sub_task_schedule_onclick;
window.button_mobile_s1_sub_task_working_start_onclick = button_mobile_s1_sub_task_working_start_onclick;
window.button_mobile_s1_sub_task_pause_onclick = button_mobile_s1_sub_task_pause_onclick;
window.button_mobile_s1_sub_task_resume_onclick = button_mobile_s1_sub_task_resume_onclick;
window.button_mobile_s1_sub_task_working_finish_onclick = button_mobile_s1_sub_task_working_finish_onclick;
window.button_mobile_s1_sub_task_verify_start_onclick = button_mobile_s1_sub_task_verify_start_onclick;
window.button_mobile_s1_sub_task_verify_fail_onclick = button_mobile_s1_sub_task_verify_fail_onclick;
window.button_mobile_s1_sub_task_fixing_start_onclick = button_mobile_s1_sub_task_fixing_start_onclick;
window.button_mobile_s1_sub_task_fixing_finish_onclick = button_mobile_s1_sub_task_fixing_finish_onclick;
window.button_mobile_s1_sub_task_verify_success_onclick = button_mobile_s1_sub_task_verify_success_onclick;
window.button_mobile_login_supervisor_onclick = button_mobile_login_supervisor_onclick;
window.button_mobile_s1_sub_task_cancel_by_field_executor_onclick = button_mobile_s1_sub_task_cancel_by_field_executor_onclick;
window.button_mobile_s1_sub_task_cancel_by_customer_onclick = button_mobile_s1_sub_task_cancel_by_customer_onclick;
window.button_mobile_sub_task_flow_sk_onclick = button_mobile_sub_task_flow_sk_onclick;
window.button_mobile_sub_task_flow_sr_onclick = button_mobile_sub_task_flow_sr_onclick;
window.button_mobile_sub_task_flow_mi_onclick = button_mobile_sub_task_flow_mi_onclick;
window.button_mobile_sub_task_flow_gi_onclick = button_mobile_sub_task_flow_gi_onclick;

await main();
