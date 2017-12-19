const fs = require('fs');
const async = require('async');
const parse = require('csv-parse/lib/sync');
const asyncParse = require('csv-parse/lib');
const es = require('event-stream');

const JSONStream = require('JSONStream');
// const getStream = require('./lib/getJsonStream');
const move = require('./lib/move');
const getJsDateFromExcel = require('./lib/getJsDateFromExcel');
const run = require('./lib/runner');

const queue = require('./lib/queue');

// MONGO
// const MongoClient = require('mongodb').MongoClient;
// const url = 'mongodb://localhost:27017/RS101';
const url = 'mongodb://localhost:C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==@localhost:10255/RS101?ssl=true';

// const conn = MongoClient.connect(url);
const mongoose = require('mongoose')

// mongoose.Promise = global.Promise;
const conn = mongoose.connect(url, {
    // config: { autoIndex: false },
    useMongoClient: true,
})
    .then((err) => {
        if (err) {
            console.log('Could NOT connect to database: ', err);
        } else {
            console.log('Connected to database');
        }
    });

// const conn = mongoose.connect(url, { config: { autoIndex: false } });
//mongoose.connect('mongodb://localhost/test', { useMongoClient: true });

const publicUserSchema = require('./lib/schema');
const PublicUser = mongoose.model('PublicUser', publicUserSchema);

// FOLDERS
const inputFolder = './csv/input/';
const csvProcessedFolder = './csv/processed/';
const csvExportFolder = './csv/export/';
const jsonInputFolder = './json/input/';
const jsonImportedFolder = './json/imported/';
const unsubFolder = {
    input: './csv/unsubscribed/input/',
    processed: './csv/unsubscribed/processed/'
}

// Book.insertMany(rawDocuments)
//     .then(function(mongooseDocuments) {
//          /* ... */
//     })
//     .catch(function(err) {
//         /* Error handling */
//     });

const importJson = () => {
    run(function* seq() {
        try {
            console.log('start');
            let files = yield fs.readdir.bind(fs, jsonInputFolder);

            // conn.then(db => {
            // Get the collection
            // const col = db.collection('REBase');
            let upsertedCount = 0;
            let count = 0;
            // files.forEach(file => {
            let file = files[0];

            console.log(`Start: ${file}`);
            let docs = []

            let stream = getStream(file)
                .pipe(es.mapSync((data) => {
                    // let ogdt = data['ORIG_DTE'] = getJsDateFromExcel(data['ORIG_DTE']);
                    // let exdt = data['EXPR_DTE'] = getJsDateFromExcel(data['EXPR_DTE']);
                    let doc = mapToPublicUser(data);
                    count++;

                    // if (doc.LIC_NBR) {

                    let User = new PublicUser(doc);
                    // User.save(function (err, res) {
                    //     if (err) return console.log(err);
                    // })

                    const promise = User.save();
                    Promise.all([promise]).then(function (err, res) {
                        if (err) return console.log(err);
                    })

                    //     .then(dt => console.log(dt))
                    //     .catch(err => console.log(err));

                    // PublicUser.create(doc, function (err, usr) {
                    //     if (err) return console.log(err);
                    //     // saved!
                    // })

                    if (count % 100 === 0) {
                        console.log(`${count}: Upserted count so far ${upsertedCount}`);
                    }
                    // } // IF LIC_NBR
                }));
            stream.on('close', () => {
                console.log(`${file} Stream Done | upsertedCount: ${upsertedCount}`)
                move(jsonInputFolder + '/' + file, jsonImportedFolder + '/' + file, (err) => {
                    console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                });
            });
            // }); // foreach
            // });
        } catch (ex) {
            return console.log(ex);
        }
    }); //RUN
}

const mapToPublicUser = (doc) => {
    return {
        License_Number: doc.LIC_NBR,
        Name: `${doc.FRST_NME} ${doc.SURNME}`,
        Email: doc.E_MAIL_ADDR.toLowerCase(),
        First_Name: doc.FRST_NME,
        Last_Name: doc.SURNME,
        Phone: doc.PHNE_NBR,
        County: doc['CNTY DESC'],
        License_Type: doc.MOD_DESC,
        License_Status: doc.LIC_SEC_STA_DESC,
        Expire_Date: getJsDateFromExcel(doc.EXPR_DTE),
        Origin_Date: getJsDateFromExcel(doc.ORIG_DTE),
        Address: {
            Street: [
                doc.STR_ADDR_NBR,
                doc.ADDR_LINE1,
                doc.ADDR_LINE2,
                doc.ADDR_LINE3,
            ].join(' ').trim(),
            City: doc.ADDR_CTY,
            State: doc.ST_CDE,
            Zip: doc.ADDR_ZIP
        }
    }
}

// const importJson = () => {
//     run(function* seq() {
//         try {
//             console.log('start');
//             let files = yield fs.readdir.bind(fs, jsonInputFolder);

//             conn.then(db => {
//                 // Get the collection
//                 const col = db.collection('REBase');
//                 let upsertedCount = 0;
//                 let count = 0;
//                 // files.forEach(file => {
//                 let file = files[0];

//                 console.log(`Start: ${file}`);

//                 let stream = getStream(file)
//                     .pipe(es.mapSync((data) => {
//                         let ogdt = data['ORIG_DTE'] = getJsDateFromExcel(data['ORIG_DTE']);
//                         let exdt = data['EXPR_DTE'] = getJsDateFromExcel(data['EXPR_DTE']);
//                         let doc = data;
//                         // Set update
//                         let up = {
//                             $set: doc
//                         };
//                         count++;

//                         if (doc.LIC_NBR) {
//                             let updt = col.updateOne({
//                                 LIC_NBR: doc.LIC_NBR
//                             },
//                                 up, {
//                                     upsert: true
//                                 });

//                             // queue.add(updt.then((data) => {
//                             //         upsertedCount += data.upsertedCount;
//                             //         if (count % 100 === 0) {
//                             //             console.log(`${count}: Upserted count so far ${upsertedCount}`);
//                             //             // count = 0;
//                             //         }
//                             //     })
//                             //     // .catch(err => console.log(`${err}`))
//                             // )
//                             // .then(data => console.log(`${data}`))
//                             // .catch(err => console.log(`${err}`))
//                             queue(updt.then(function (data) {
//                                 upsertedCount += data.upsertedCount;
//                             })
//                             );

//                             if (count % 100 === 0) {
//                                 console.log(`${count}: Upserted count so far ${upsertedCount}`);
//                                 // count = 0;
//                             }
//                         } // IF LIC_NBR
//                     }));
//                 stream.on('close', () => {
//                     console.log(`${file} Stream Done | upsertedCount: ${upsertedCount}`)
//                     move(jsonInputFolder + '/' + file, jsonImportedFolder + '/' + file, (err) => {
//                         console.log(err ? 'ERROR: ' + err : "Moved: " + file);
//                     });
//                 });
//                 // }); // foreach
//             });
//         } catch (ex) {
//             return console.log(ex);
//         }
//     }); //RUN
// }

importJson();

const getStream = (file) => {
    let jsonData = file ?
        jsonInputFolder + file :
        jsonInputFolder + 'DRE_Licensee_2501_Addr_File1_0517.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse('RE PRR Licensee\'s Address Phone.*'); //'*'
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

const removeUnsubscribers = () => {
    // process input files
    fs.readdir(unsubFolder.input, (err, files) => {
        // files.forEach(file => {
        var file = files[0];
        console.log(file);

        if (!file.includes('.csv')) {
            return console.log('ERROR .csv');
        }

        var parser = asyncParse({
            // columns: true,
            delimiter: ','
        });
        let newFilename = jsonInputFolder + file.replace('.csv', '.json')
        var input = fs.createReadStream(unsubFolder.input + '/' + file);

        // // Write to new .json file
        // var writer = fs.createWriteStream(newFilename, {
        //     flags: 'a', // 'a' means appending (old data will be preserved)
        //     encoding: 'utf-8'
        // })

        let upsertedCount = 0;
        let count = 0;
        // Use the writable stream api
        parser.on('readable', function () {
            while (record = parser.read()) {
                let name = record[0];
                let email = record[1];
                console.log(email);
                if (email) {
                    conn.then(db => {
                        // Get the collection
                        const col = db.collection('REUnsubscribers');
                        let up = {
                            $set: {
                                'Email': email,
                                'Name': name
                            }
                        };
                        let updt = col.updateOne({
                            'Email': email
                        },
                            up, {
                                upsert: true
                            });

                        updt.then(function (data) {
                            count++;
                            upsertedCount += data.upsertedCount;
                            console.log(`${count}: Upserted count so far ${upsertedCount}`);
                        }, (err) => console.log('Error:', err));

                    });
                }
                // writer.write(JSON.stringify(record) + '\n') // append string to your file    
            }
        });
        // Catch any error
        parser.on('error', function (err) {
            console.log(err.message);
        });

        parser.on('finish', () => {
            parser.end()
            console.log('PARSER FINISH *** ', file);
            // // console.log("The file was saved! ", newFilename);
            move(unsubFolder.input + '/' + file, unsubFolder.processed + '/' + file, (err) => {
                console.log(err ? 'ERROR: ' + err : "Moved: " + file);
            });
        })

        input.pipe(parser);

        // return;
        // }); // foreach

    })
}

// removeUnsubscribers();

const exportList = (list = 'Expire-Sept-30-2017') => {
    console.log('Start Export', list);
    conn.then(db => {
        const col = db.collection(list);
        let ppl = col.find({}).map(data => {
            return {
                Name: data.Name,
                Email: data.Email
            }
        });

        let newFile = csvExportFolder + list + '.csv';
        // Write to new .json file
        var writer = fs.createWriteStream(newFile, {
            flags: 'a', // 'a' means appending (old data will be preserved)
            encoding: 'utf-8'
        });

        ppl.forEach((data) => {
            let row = `${data.Name}, ${data.Email}\n`;
            console.log(row);
            writer.write(row);
        });
    });
}

// exportList();