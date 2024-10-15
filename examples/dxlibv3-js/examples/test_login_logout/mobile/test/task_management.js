import * as apiMobile from "../../library/api_mobile.js";

export async function subtask_pick(keys, sub_task_id) {
  let sub_task = await apiMobile.sub_task_read(keys, sub_task_id);
  let result = await apiMobile.sub_task_pick(keys, sub_task.id);
  console.log(result);
  return sub_task;
}

export async function sub_task_schedule(keys, sub_task_id) {
  let start_data = dayjs().format("YYYY-MM-DD");
  let end_data = dayjs().add(1, "month").format("YYYY-MM-DD");
  let result = await apiMobile.sub_task_schedule(keys, sub_task_id, start_data, end_data);
  console.log(result);
}

export async function sub_task_cancel_by_field_executor(keys, sub_task_id) {
  let result = await apiMobile.sub_task_cancel_by_field_executor(keys, sub_task_id, dayjs().toISOString(), {
    reason: "alasan cancel",
  });
  console.log(result);
}

export async function sub_task_cancel_by_customer(keys, sub_task_id) {
  let result = await apiMobile.sub_task_cancel_by_customer(keys, sub_task_id, dayjs().toISOString(), {
    reason: "alasan cancel",
  });
  console.log(result);
}

export async function sub_task_pause(keys, sub_task_id) {
  let result = await apiMobile.sub_task_pause(keys, sub_task_id, dayjs().toISOString(), {
    reason: "alasan tunda",
    field_supervisor_fullname: "SUNARYA",
  });
  console.log(result);
}

export async function sub_task_resume(keys, sub_task_id) {
  let result = await apiMobile.sub_task_resume(keys, sub_task_id, dayjs().toISOString());
  console.log(result);
}

export async function sub_task_working_start(keys, sub_task_id) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_working_start(keys, sub_task_id, at);
  console.log(result);
}

export async function sub_task_working_finish(keys, sub_task_id, report) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_working_finish(
    keys,
    sub_task_id,
    at,
    report ?? {
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
    }
  );
  console.log(result);
}

export async function sub_task_verify_start(keys, sub_task_id) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_verify_start(keys, sub_task_id, at);
  console.log(result);
}

export async function sub_task_verify_success(keys, sub_task_id) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_verify_success(keys, sub_task_id, at);
  console.log(result);
}

export async function sub_task_verify_fail(keys, sub_task_id) {
  let at = dayjs().toISOString();
  let report = {
    reason: "Verifikasi Di Tolak",
    remedial_action: "Pemasangan tidak sesuai standart",
  };
  let result = await apiMobile.sub_task_verify_fail(keys, sub_task_id, at, report);
  console.log(result);
}

export async function sub_task_fixing_start(keys, sub_task_id) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_fixing_start(keys, sub_task_id, at);
  console.log(result);
}

export async function sub_task_fixing_finish(keys, sub_task_id, report) {
  let at = dayjs().toISOString();
  let result = await apiMobile.sub_task_fixing_finish(
    keys,
    sub_task_id,
    at,
    report ?? {
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
    }
  );
  console.log(result);
}
