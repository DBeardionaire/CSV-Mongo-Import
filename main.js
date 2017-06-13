const fs = require('fs');
const async = require('async');
const move = require('./lib/move');
const getJsDateFromExcel = require('./lib/getJsDateFromExcel');
const parse = require('csv-parse/lib/sync');
const JSONStream = require('JSONStream');
const es = require('event-stream');
const run = require('./lib/runner');

// const monk = require('monk');
// var db = monk('localhost:27017/RS101');

// db.then(() => {
//   console.log('Connected correctly to server')
// })

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

const url = 'mongodb://localhost:27017/RS101';
// FOLDERS
const inputFolder = './csv/input/';
const csvProcessedFolder = './csv/processed/';
const jsonInputFolder = './json/input/';
const jsonImportedFolder = './json/imported/';



const main = () => {
    run(function* seq() {
        try {
            console.log('start 1');
            var files = yield fs.readdir.bind(fs, jsonInputFolder);
            console.log('end 1');
            MongoClient.connect(url, function (err, db) {
                assert.equal(null, err);

                // for (let file of files) {
                //     console.log(file);
                    var stream = getStream() //file
                        .pipe(es.mapSync((data) => {
                            // console.log(data[10].E_MAIL_ADDR);
                            for (let i = 0; i < data.length; i++) {
                                // IF E_MAIL_ADDR &| PHone       
                                // console.log(data[i]);                 
                                delete data[i][''];
                                let ogdt = data[i]['ORIG_DTE'] = getJsDateFromExcel(data[i]['ORIG_DTE']); 
                                let exdt = data[i]['EXPR_DTE'] = getJsDateFromExcel(data[i]['EXPR_DTE']);
                                updateDoc(db, data[i], (i == data.length - 1), () => {
                                    console.log('----TEST-----', i)
                                });
                            }
                        }));

                // }
                console.log('END MongoClient Loop')
            });

        } catch (ex) {
            return console.log(ex);
        }
    }); //RUN
}

const importJson = () => {
    run(function* seq() {
        try {
            console.log('readdir: ', jsonInputFolder);
            var files = yield fs.readdir.bind(fs, jsonInputFolder);
            // MongoClient 
            MongoClient.connect(url, function (err, db) {
                assert.equal(null, err);

                for (let file of files) {
                    console.log(file);
                    var stream = getStream(file) //jsonInputFolder + 
                        .pipe(es.mapSync((data) => {
                            // Loop
                            for (let i = 0; i < data.length; i++) {
                                // IF E_MAIL_ADDR &| PHone                        
                                // console.log(data[i].E_MAIL_ADDR);
                                delete data[i][''];
                                let ogdt = data[i]['ORIG_DTE'] = getJsDateFromExcel(data[i]['ORIG_DTE']); 
                                let exdt = data[i]['EXPR_DTE'] = getJsDateFromExcel(data[i]['EXPR_DTE']);
                                // Call Update
                                updateDoc(db, data[i], (i == data.length - 1), () => {
                                    console.log('----TEST-----', i)
                                });
                            }
                            console.log('END data Loop')            
                        }));

                }
                console.log('END MongoClient Loop')
            });

        } catch (ex) {
            return console.log(ex);
        }
    }); //RUN
}

main();

// const getJsDateFromExcel = (excelDate) => {

//     // JavaScript dates can be constructed by passing milliseconds
//     // since the Unix epoch (January 1, 1970) example: new Date(12312512312);

//     // 1. Subtract number of days between Jan 1, 1900 and Jan 1, 1970, plus 1 (Google "excel leap year bug")             
//     // 2. Convert to milliseconds.
//     // let dt = new Date((excelDate - (25567 + 1)) * 86400 * 1000);
//     return new Date((excelDate - (25567 + 1)) * 86400 * 1000);
// }

const getStream = (file) => {
    let jsonData = file ?
         jsonInputFolder + file
         : jsonInputFolder + 'DRE_Licensee_2501_Addr_File1_0517.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse('*');
    return stream.pipe(parser);
};



const updateDoc = function (db, doc, isLastUpdate, callback) {
    console.log(doc);
    // Set update
    let up = {
        $set: doc
    };
    
    // // Set dates
    // up = {
    //     $set: {
    //         "ORIG_DTE": doc.ORIG_DTE,
    //         "EXPR_DTE": doc.EXPR_DTE
    //     }
    // }

    // Get the collection
    var col = db.collection('REBase');
    col.updateOne({
        LIC_NBR: doc.LIC_NBR
    }, 
        up, 
    {
        upsert: true
    }, function (err, r) {
        assert.equal(null, err);
        // assert.equal(1, r.matchedCount);
        // assert.equal(1, r.upsertedCount);
        // Finish up assert
        if (isLastUpdate) {
            db.close();
        }
    }, function (err, results) {
        console.log(results);
        callback();
    });

};



// Step 1 after py script
const csv2json = () => {
    // process input files
    fs.readdir(inputFolder, (err, files) => {
        files.forEach(file => {
        // var file = files[1];
        console.log(file);

        if (!file.includes('.csv')) {
            return console.log('ERROR .csv');
        }

        fs.readFile(inputFolder + '/' + file, 'utf-8', function (err, data) {
            if (err) {
                return console.log(err)
            }

            try {
                // parse csv to json
                let json = parse(data, {
                    columns: true
                });
                console.log(json);

                // var newJson = items.map(item => ({
                //     name: item.title,
                //     id: item.uid
                // }));

                let newFilename = jsonInputFolder + file.replace('.csv', '.json');
                // Write to new .json file
                fs.writeFile(newFilename, JSON.stringify(json), {
                        encoding: 'utf-8'
                    },
                    function (err) {
                        if (err) {
                            return console.log(err);
                        }
                        console.log("The file was saved! ", newFilename);
                        move(inputFolder + '/' + file, csvProcessedFolder + '/' + file, (err) => {
                            console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                        });
                    });
            } catch (ex) {
                return console.log(ex)
            }
        })

        }); // foreach

    })
};