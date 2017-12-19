const JSONStream = require('JSONStream');
const fs = require('fs')

const getStream = (file) => {
    let jsonData = file ?
        jsonInputFolder + file :
        jsonInputFolder + 'DRE_Licensee_2501_Addr_File1_0517.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse('obj.$*'); //'*'
    return stream.pipe(parser);
};

// exports = getStream;