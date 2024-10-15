import * as apiWebadmin from '../../library/api_webadmin.js';

export async function init_data_announcement(keys) {
    await apiWebadmin.announcement_create(keys,
        "", "Cara Beli Voucher Gas PGN",
        `
Loren ipsum dolor sit amet, consectetur adipisciång elit. Loren ipsum dolor sit amet, consectetur adipiscing elit. Loren ipsum dolor sit amet, consectetur adipiscing elit1.
Loren ipsum dolor sit amet, consectetur adipiscing elit. Loren ipsum dolor sit amet, consectetur adipiscing elit. Loren ipsum dolor sit amet, consectetur adipiscing elit2.
Loren ipsum dolor sit amet, consectetur adipiscing elit. Loren ipsum dolor sit amet, consectetur adipiscing elit. Loren ipsum dolor sit amet, consectetur adipiscing elit3.
`
    );

    let l = await apiWebadmin.announcement_list(keys,
        "", "", {}, 0, 0, false
    )
    console.log(l)
}
