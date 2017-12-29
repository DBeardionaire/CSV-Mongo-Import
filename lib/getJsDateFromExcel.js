const moment = require('moment');
//
const getJsDateFromExcel = (date) => {
    if (typeof date === 'string') {
        try {
            let dt = moment.utc(date)
            if (dt.isValid()) {
                return dt
            }
            return (null)
            // throw new Error(`Date not valid: ${date} | ${dt}`)
        } catch (e) {
            console.log(e)
        }
    }
    // JavaScript dates can be constructed by passing milliseconds
    // since the Unix epoch (January 1, 1970) example: new Date(12312512312);

    // 1. Subtract number of days between Jan 1, 1900 and Jan 1, 1970, plus 1 (Google "excel leap year bug")             
    // 2. Convert to milliseconds.
    // let dt = new Date((excelDate - (25567 + 1)) * 86400 * 1000);
    return new Date((date - (25567 + 1)) * 86400 * 1000);
}

module.exports = getJsDateFromExcel;