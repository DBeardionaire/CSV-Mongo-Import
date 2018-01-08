const fs = require('fs');
const async = require('async');
const parse = require('csv-parse/lib/sync');
const asyncParse = require('csv-parse/lib');
const csvStringifySync = require('csv-stringify/lib/sync');
const es = require('event-stream');
const JSONStream = require('JSONStream');
// const getStream = require('./lib/getJsonStream');
const move = require('./lib/move');
const getJsDateFromExcel = require('./lib/getJsDateFromExcel');
const run = require('./lib/runner');
const node_xj = require("xlsx-to-json");

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
const xlsxInputFolder = './xlsx/input/';
const xlsxOutputFolder = './xlsx/output/';
const xlsxProcessedFolder = './xlsx/processed/';

// MONGO
// const MongoClient = require('mongodb').MongoClient;
const url = 'mongodb://localhost:27017/RS101';
// const url = 'mongodb://localhost:C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==@localhost:10255/RS101?ssl=true';

// const conn = MongoClient.connect(url);
const mongoose = require('mongoose')
mongoose.Promise = require('bluebird');
// mongoose.set('debug', true);
const options = {
    // useMongoClient: true,
    autoIndex: false, // Don't build indexes
    // reconnectTries: Number.MAX_VALUE, // Never stop trying to reconnect
    // reconnectInterval: 500, // Reconnect every 500ms
    // poolSize: 10, // Maintain up to 10 socket connections
    // If not connected, return errors immediately rather than waiting for reconnect
    // bufferMaxEntries: 0
};

const conn = mongoose.connect(url, options)
    .then((err) => {
        if (err) {
            console.log('Could NOT connect to database: ', err);
        } else {
            console.log('Connected to database');
            // tagUnsubscribers();
            // exportListforFB();
        }
    });

const schemas = require('./lib/schemas');
const UNSUB_ENUM = schemas.UNSUB_ENUM;
const publicUserSchema = schemas.publicUserSchema
const PublicUser = mongoose.model('PublicUser', publicUserSchema);
const unsubscriberSchema = schemas.unsubscriberSchema
const Unsubscriber = mongoose.model('Unsubscriber', unsubscriberSchema);

// TODO: Migration
// var updates = { $unset: { steak: true }, $set: { eggs: 0 } };
// // Note the `runValidators` option
// Breakfast.update({}, updates, { runValidators: true }, function(err) {
//   console.log(err.errors['steak'].message);
//   console.log(err.errors['eggs'].message);
//   /*
//    * The above error messages output:
//    * "Path `steak` is required."
//    * "Path `eggs` (0) is less than minimum allowed value (2)."
//    */
// });

const importJson = () => {
    run(function* seq() {
        try {
            console.log('start');
            let files = yield fs.readdir.bind(fs, jsonInputFolder);

            let insertedCount = 0;
            let upsertedCount = 0;
            let count = 0;
            files.forEach(file => {
                // let file = files[0];
                console.log(`Start: ${file}`);

                let stream = getStream(jsonInputFolder + file)
                    .pipe(es.mapSync((data) => {
                        let doc = mapToPublicUser(data);

                        isUnsubscriber(doc).then(isUnsub => {
                            if (doc && doc.License_Number && doc.Email) {
                                doc.Unsubscribe = Boolean(isUnsub);
                                doc.DoNotSend = isUnsub;
                                
                                PublicUser.findOneAndUpdate(
                                    {
                                        License_Number: doc.License_Number,
                                        //Email: doc.Email 
                                    }, // find by
                                    doc,
                                    { upsert: true, new: false, runValidators: true },
                                    function (err, user) { // callback
                                        count++;
                                        if (err) {
                                            return console.log(err);
                                        } else if (user && !user.isNew) {
                                            upsertedCount++
                                        } else {
                                            insertedCount++;
                                        }
                                        if (count % 100 === 0) {
                                            console.log(`${count}: Inserted: ${insertedCount} | Upserted ${upsertedCount}`);
                                        }
                                    }
                                )
                            }
                        })
                    }));

                stream.on('close', () => {
                    console.log(`${file} Stream Done | Count: ${count} | upsertedCount: ${upsertedCount}`)
                    move(jsonInputFolder + '/' + file, jsonImportedFolder + '/' + file, (err) => {
                        console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                    });
                });
            }); // foreach
            // });
        } catch (ex) {
            return console.log(ex);
        }
    }); //RUN
}

function phoneStrip(ph) {
    try {
        if (ph && typeof ph === 'string') {
            return ph.replace(/[^\d]/g, '')
        }
    } catch (e) {
        console.log(e)
    }
    return ph;
}

function toValidEmail(email) {
    try {
        if (email && typeof email === 'string') {
            return email.replace(/\s/g, "").toLowerCase()
        }
    } catch (e) {
        console.log(e)
    }
    return email
}

const mapToPublicUser = (doc) => {
    return {
        License_Number: doc.LIC_NBR,
        Name: `${doc.FRST_NME} ${doc.SURNME}`,
        Email: toValidEmail(doc.E_MAIL_ADDR),
        First_Name: doc.FRST_NME,
        Last_Name: doc.SURNME,
        Phone: phoneStrip(doc.PHNE_NBR),
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
        },
        Unsubscribe: false, // TODO: set from db
        DoNotSend: '',
    }
}

const getStream = (file) => {
    let jsonData = file ?
        file :
        jsonInputFolder + 'DRE_Licensee_2501_Addr_File1_0517.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse('*'); //'*' // 'RE PRR Licensee\'s Address Phone.*'
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

const getUnsubType = (filename) => {
    if (filename.includes('unsub')) {
        return UNSUB_ENUM[1]
    } else if (filename.includes('bounce')) {
        return UNSUB_ENUM[2]
    } else if (filename.includes('spam')) {
        return UNSUB_ENUM[3]
    } else {
        return '';
    }
}

const uploadUnsubscribers = () => {
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

        let upsertedCount = 0;
        let count = 0;
        // Use the writable stream api
        parser.on('readable', function () {
            while (record = parser.read()) {
                let name = record[0];
                let email = record[1];
                console.log(email);
                if (email) {
                    let UnsubscribeType = getUnsubType(file);
                    let unsub = new Unsubscriber({
                        Name: name,
                        Email: email,
                        UnsubscribeType
                    })
                    unsub.save(function (err, doc) {
                        if (err) return console.log(err)

                        if (doc && doc.Email) {
                            PublicUser.update({
                                Email: doc.Email,
                            },
                                { 
                                    Unsubscribe: true,
                                    DoNotSend: UnsubscribeType
                                },
                                { multi: true, runValidators: true }, function (error, raw) {
                                    if (error) return console.log(error)
                                    console.log('The raw response from Mongo was ', raw);
                                });
                        }
                    })
                }
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

const isUnsubscriber = (doc) => {
    return Unsubscriber.findOne({ Email: doc.Email }, function (err, unsub) {
        if (!err) {
            if (unsub) {                
                return unsub.UnsubscribeType || UNSUB_ENUM[1]; // Defaults to UNSUBSCRIBE
            }
        }

        return false;
    });
}

const tagUnsubscribers = () => {
    Unsubscriber.find({}).then(function (unsubs) {
        unsubs.forEach(unsub => {
            PublicUser.update({
                Email: unsub.Email,
            },
                { // UPDATE TO
                    Unsubscribe: true,
                    DoNotSend: unsub.UnsubscribeType || UNSUB_ENUM[1],
                },
                { multi: true, runValidators: true }, function (error, raw) {
                    if (error) 
                        return console.log(error)

                    console.log('The raw response from Mongo was ', raw);
                })
        }) // Foreach
    })
}

const exportListfromDB = (list = 'Sales-Expire-Mar-31-2018') => {
    console.log('Start Export', list);

    const batch = 5000;
    let writtenEmails = [];
    let num = 1;
    let counts = {
        [num]: 0
    }
    const getfileName = () => `${csvExportFolder}${list}_${num}.csv`
    // Write to new .json file
    const getWriter = () => fs.createWriteStream(getfileName(), {
        flags: 'w', // 'a' means appending (old data will be preserved)
        encoding: 'utf-8'
    });

    const run = (skipNum = 0) => {
        let writer = getWriter();
        var cursor = PublicUser.find({
            Unsubscribe: false
        })
            // .where()
            .and([
                { Expire_Date: new Date("2018-03-31T16:00:00.000-08:00") },
                { License_Type: "Sales Associate" },
            ])
            // .or([
            //     { License_Type: "Broker Sales" },
            //     { License_Type: "Broker" }
            // ])
            .skip(skipNum)
            .limit(batch) // cant be used w/distinct
            .select('Name Email') // cant be used w/distinct
            .cursor();

        cursor.on('data', function (doc) {
            let contains = writtenEmails.some(e => e === doc.Email);
            let row = `${doc.Name.replace(/,/g, '')},${doc.Email}\n`;
            if (contains) {
                console.log(`Duplicate: ${row}`);
                writtenEmails.push(doc.Email);
                return;
            }
            // console.log(row);
            writtenEmails.push(doc.Email);
            writer.write(row);
            counts[num]++;
        });

        cursor.on('close', function () {
            console.log('END Export', getfileName());
            let check = counts[num]
            if (check && check > 0) {
                num++;
                writer = getWriter();
                counts[num] = 0;
                run(writtenEmails.length);
            }
        });
    }
    run();
}

const exportListforFB = (list = 'Sendy-Campaign-Broker-Expire-Mar-31-2018') => {
    console.log('Start Export', list);

    const batch = 15000;
    let rows = []; // for csv stringifier
    let writtenEmails = [];
    let num = 1;
    let counts = {
        [num]: 0
    }
    const getfileName = () => `${csvExportFolder}${list}_${num}.csv`
    // Write to new .json file
    const getWriter = () => fs.createWriteStream(getfileName(), {
        flags: 'w', // 'a' means appending (old data will be preserved)
        encoding: 'utf-8'
    });

    const run = (skipNum = 0) => {
        let selectCols = 'First_Name Last_Name Email Phone Address.City Address.State Address.Zip'
        //Expire_Date Address.City Address.State Address.Zip'
        let writer = getWriter();
        var cursor = PublicUser.find({
            Unsubscribe: false
        })
            // .where()
            .and([
                { Expire_Date: new Date("2018-03-31") }, //T16:00:00.000-08:00
                // { License_Type: "Sales Associate" },
            ])
            .or([
                { License_Type: "Broker Sales" },
                { License_Type: "Broker" }
            ])
            .skip(skipNum)
            .limit(batch) // cant be used w/distinct
            // .select('Name Email') // cant be used w/distinct
            .select(selectCols) // cant be used w/distinct
            .cursor();

        cursor.on('data', function (doc) {
            let contains = writtenEmails.some(e => e === doc.Email);
            let row = doc.toObject({ hide: '_id' });
            // let row = csvStringifySync(obj,{ header: true }); // `${doc.Name.replace(/,/g, '')},${doc.Email}\n`;
            if (contains) {
                console.log(`Duplicate: ${row.Email}`);
                writtenEmails.push(doc.Email);
                return;
            }
            // console.log(row);
            writtenEmails.push(doc.Email);
            // writer.write(row);
            row.City = row.Address.City
            row.State = row.Address.State;
            row.Zip = row.Address.Zip;
            delete row.Address;
            rows.push(row);
            counts[num]++;
        });

        cursor.on('close', function () {
            if (rows.length) {
                let csv = csvStringifySync(rows,{ header: true });
                writer.write(csv);
            }
            console.log('END Export', getfileName());
            let check = counts[num]
            if (check && check > 0) {
                num++;
                writer = getWriter();
                counts[num] = 0;
                rows = [];
                run(writtenEmails.length);
            }
        });
    }
    run();
}

const xlsx2Json = () => {
    let files = fs.readdirSync(xlsxInputFolder);

    async.eachLimit(files, 1, function (file, cb) {
        let input = xlsxInputFolder + file
        let output = xlsxOutputFolder + file.replace(".xlsx", "_output.json")

        node_xj({
            input,
            output,
            // sheet: "sheetname"  // specific sheetname 
        }, function (err, result) {
            if (err) {
                console.error(err);
            } else if (result && result.length) {
                console.log(result.length);
            }
            cb();
        });
    }, function (res) {
        console.log(res);
        async.each(files, function (file) {
            move(xlsxInputFolder + file, xlsxProcessedFolder + file, (err) => {
                console.log(err ? 'ERROR: ' + err : "Moved: " + file);
            });
        })
    })
}

const importJsonX = () => {
    try {
        let files = fs.readdirSync(xlsxOutputFolder);

        async.eachLimit(files, 1, function (file, cb) {
            console.log(`importing: ${file}`);
            let insertedCount = 0;
            let upsertedCount = 0;
            let count = 0;

            let stream = getStream(xlsxOutputFolder + file)
                .pipe(es.mapSync((data) => {
                    let doc = mapToPublicUser(data);

                    isUnsubscriber(doc).then(isUnsub => {
                        if (doc && doc.License_Number && doc.Email) {
                            doc.Unsubscribe = isUnsub ? true : false;

                            PublicUser.findOneAndUpdate(
                                {
                                    License_Number: doc.License_Number,
                                    //Email: doc.Email 
                                }, // find by
                                doc,
                                { upsert: true, new: false, runValidators: true },
                                function (err, user) { // callback
                                    count++;
                                    if (err) {
                                        return console.log(err);
                                    } else if (user && !user.isNew) {
                                        upsertedCount++
                                    } else {
                                        insertedCount++;
                                    }
                                    if (count % 100 === 0) {
                                        console.log(`${count}: Inserted: ${insertedCount} | Upserted ${upsertedCount}`);
                                    }
                                }
                            )
                        }
                    })
                }));

            stream.on('close', () => {
                console.log(`${file} Stream Done | Count: ${count} | upsertedCount: ${upsertedCount}`)
                move(xlsxOutputFolder + '/' + file, jsonImportedFolder + '/' + file, (err) => {
                    console.log(err ? 'ERROR: ' + err : "Moved: " + file);
                });
                cb(); // NEXT
            });
        }, function end(err) {
            console.log(`finished importing`);
        });
    } catch (ex) {
        return console.log(ex);
    }
}