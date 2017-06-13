//
const getJsDateFromExcel = (excelDate) => {

    // JavaScript dates can be constructed by passing milliseconds
    // since the Unix epoch (January 1, 1970) example: new Date(12312512312);

    // 1. Subtract number of days between Jan 1, 1900 and Jan 1, 1970, plus 1 (Google "excel leap year bug")             
    // 2. Convert to milliseconds.
    // let dt = new Date((excelDate - (25567 + 1)) * 86400 * 1000);
    return new Date((excelDate - (25567 + 1)) * 86400 * 1000);
}

module.exports = getJsDateFromExcel;