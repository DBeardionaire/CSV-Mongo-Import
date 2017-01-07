const fs = require('fs');
const move = require('./lib/move');
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


const main1 = () => {
    // process input files
    fs.readdir(inputFolder, (err, files) => {
        // files.forEach(file => {
        var file = files[1];
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

        //}); // foreach

    })
};

const main = () => {
    run(function* seq() {
        try {
            console.log('start 1');
            var files = yield fs.readdir.bind(fs, jsonInputFolder);
            console.log('end 1');
            MongoClient.connect(url, function (err, db) {
                assert.equal(null, err);

                for (let file of files) {
                    console.log(file);
                    var stream = getStream(file)
                        .pipe(es.mapSync((data) => {
                            // console.log(data[10].E_MAIL_ADDR);
                            for (let i = 0; i < data.length; i++) {
                                // IF E_MAIL_ADDR &| PHone                        
                                delete data[i][''];
                                updateDoc(db, data[i], (i == data.length - 1), () => {
                                    console.log('----TEST-----', i)
                                });
                            }
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
const importJson = (db, folder = jsonInputFolder) => {

    fs.readdir(folder, (err, files) => {
        if (err) {
            return console.log(err)
        }

        // var file = [].push(files[1]);
        // console.log(file);

        var files = files.filter((file) => {
            if (!file.includes('.json')) {
                console.log('ERROR not .json file');
            }
            return (file.includes('.json'));
        });
        // in sync
        for (let f = 0; f < files.length; f++) {
            // sequentialCallback();
            getStream(files[f])
                .pipe(es.mapSync((data) => {
                    // console.log(data[10].E_MAIL_ADDR);
                    for (let i = 0; i < data.length; i++) {
                        // IF E_MAIL_ADDR &| PHone                        
                        delete data[i][''];
                        updateDoc(db, data[i], (i == data.length - 1), () => {
                            console.log('----TEST-----', i)
                        });
                    }
                    // // Move to imported folder
                    // move(jsonInputFolder + files[f], jsonImportedFolder + files[f], (err) => {
                    //     console.log(err ? 'ERROR: ' + err : "Moved: " + files[f]);
                    // });
                }));
        } // END FOREACH

    })

};


const getStream = (file) => {
    let jsonData = file ?
        jsonInputFolder + file :
        jsonInputFolder + 'RE_Licensee_2501_Addr_File1_0117.json',
        stream = fs.createReadStream(jsonData, {
            encoding: 'utf8'
        }),
        parser = JSONStream.parse('*');
    return stream.pipe(parser);
};



const updateDoc = function (db, doc, isLastUpdate, callback) {
    // Get the collection
    var col = db.collection('REBase');
    col.updateOne({
        LIC_NBR: doc.LIC_NBR
    }, {
        $set: doc
    }, {
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


const insertDocument = function (db, callback) {
    db.collection('restaurants').insertOne({
        "address": {
            "street": "2 Avenue",
            "zipcode": "10075",
            "building": "1480",
            "coord": [-73.9557413, 40.7720266]
        },
        "borough": "Manhattan",
        "cuisine": "Italian",
        "grades": [{
                "date": new Date("2014-10-01T00:00:00Z"),
                "grade": "A",
                "score": 11
            },
            {
                "date": new Date("2014-01-16T00:00:00Z"),
                "grade": "B",
                "score": 17
            }
        ],
        "name": "Vella",
        "restaurant_id": "41704620"
    }, function (err, result) {
        assert.equal(err, null);
        console.log("Inserted a document into the restaurants collection.");
        callback();
    });
};

var updateRestaurants = function (db, callback) {
    db.collection('restaurants').replaceOne({
            "restaurant_id": "41704620"
        }, {
            "name": "Vella 2",
            "address": {
                "coord": [-73.9557413, 40.7720266],
                "building": "1480",
                "street": "2 Avenue",
                "zipcode": "10075"
            }
        },
        function (err, results) {
            console.log(results);
            callback();
        });
};


function callbackToPromise(method, ...args) {
    return new Promise(function (resolve, reject) {
        return method(...args, function (err, result) {
            return err ? reject(err) : resolve(result);
        });
    });
}

function sequentialCallback(...fns) {
    return (...args) => {
        const done = args.pop()
        const next = (error, ...args) => {
            if (error) return done(error)
            if (fns.length) {
                const fn = fns.shift()
                return fn(...args, next)
            }
            return done(null, ...args)
        }
        return next(null, ...args)
    }
}

//
// db.yourCollection.aggregate([
//     { "$group": {
//         "_id": { "yourDuplicateKey": "$yourDuplicateKey" },
//         "dups": { "$push": "$_id" },
//         "count": { "$sum": 1 }
//     }},
//     { "$match": { "count": { "$gt": 1 } }}
// ]).forEach(function(doc) {
//     doc.dups.shift();
//     db.yourCollection.remove({ "_id": {"$in": doc.dups }});
// });

// db.test.findAndModify({
// ... query: {"id": "test_object"},
// ... update: {"$set": {"some_key.param2": "val2_new", "some_key.param3": "val3_new"}},
// ... new: true
// ... })
// {
//     "_id" : ObjectId("56476e04e5f19d86ece5b81d"),
//     "id" : "test_object",
//     "some_key" : {
//         "param1" : "val1",
//         "param2" : "val2_new",
//         "param3" : "val3_new"
//     }
// }

//
// db.collection.update(  { _id:...} , { $set: { 'key.another_key' : new_info  } } );

// function update(prop, newVal) {
//   const str = `profile.${prop}`;
//   db.collection.update( { _id:...}, { $set: { [str]: newVal } } );
// }