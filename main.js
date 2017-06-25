const fs = require('fs');
const async = require('async');
const parse = require('csv-parse/lib/sync');
const asyncParse = require('csv-parse/lib');
const JSONStream = require('JSONStream');
const es = require('event-stream');
const move = require('./lib/move');
const getJsDateFromExcel = require('./lib/getJsDateFromExcel');
const run = require('./lib/runner');

// MONGO
const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017/RS101';
const conn = MongoClient.connect(url);

// FOLDERS
const inputFolder = './csv/input/';
const csvProcessedFolder = './csv/processed/';
const jsonInputFolder = './json/input/';
const jsonImportedFolder = './json/imported/';


const importJson = () => {
    run(function* seq() {
        try {
            console.log('start');
            let files = yield fs.readdir.bind(fs, jsonInputFolder);

            conn.then(db => {
                // Get the collection
                const col = db.collection('REBase');
                let upsertedCount = 0;
                let count = 0;
                let file = files[0];
                console.log(`Start: ${file}`);

                let stream = getStream(file)
                    .pipe(es.mapSync((data) => {
                        let ogdt = data['ORIG_DTE'] = getJsDateFromExcel(data['ORIG_DTE']);
                        let exdt = data['EXPR_DTE'] = getJsDateFromExcel(data['EXPR_DTE']);
                        let doc = data;
                        // Set update
                        let up = {
                            $set: doc
                        };
                        if (doc.LIC_NBR) { //&& count < 1
                            let updt = col.updateOne({
                                    LIC_NBR: doc.LIC_NBR
                                },
                                up, {
                                    upsert: true
                                });

                            updt.then(function (data) {
                                upsertedCount += data.upsertedCount;
                            });

                            count++;
                            if (count % 100 === 0) {
                                console.log(`${count}: Upserted count so far ${upsertedCount}`);
                                // count = 0;
                            }
                        } else {
                            count++
                        }

                    }));
                stream.on('close', () => {
                    console.log(`${file} Stream Done | upsertedCount: ${upsertedCount}`)
                    move(jsonInputFolder + '/' + file, jsonImportedFolder + '/' + file, (err) => {
                        console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                    });
                });
            });
        } catch (ex) {
            return console.log(ex);
        }
    }); //RUN
}

// importJson();

const getStream = (file) => {
    let jsonData = file ?
        jsonInputFolder + file :
        jsonInputFolder + 'DRE_Licensee_2501_Addr_File1_0517.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse(); //'*'
    return stream.pipe(parser);
};


// Step 1 after py script
const csv2json = () => {
    // process input files
    fs.readdir(inputFolder, (err, files) => {
        files.forEach(file => {
            //var file = files[0];
            console.log(file);

            if (!file.includes('.csv')) {
                return console.log('ERROR .csv');
            }

            var parser = asyncParse({
                columns: true,
                delimiter: ','
            });
            let newFilename = jsonInputFolder + file.replace('.csv', '.json')
            var input = fs.createReadStream(inputFolder + '/' + file);

            // Write to new .json file
            var writer = fs.createWriteStream(newFilename, {
                flags: 'a', // 'a' means appending (old data will be preserved)
                encoding: 'utf-8'
            })

            // Use the writable stream api
            parser.on('readable', function () {
                while (record = parser.read()) {
                    // console.log(record);
                    writer.write(JSON.stringify(record) + '\n') // append string to your file    
                }
            });
            // Catch any error
            parser.on('error', function (err) {
                console.log(err.message);
            });

            parser.on('finish', () => {
                parser.end()
                console.log('PARSER FINISH *** ', file);
                console.log("The file was saved! ", newFilename);
                move(inputFolder + '/' + file, csvProcessedFolder + '/' + file, (err) => {
                    console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                });
            })

            input.pipe(parser);

            return;
        }); // foreach

    })
};

// csv2json();