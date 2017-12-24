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

// importJson();

// // Depricated
// const exportList = (list = 'Expire-Mar-31-2018') => {
//     console.log('Start Export', list);
//     conn.then(db => {
//         const col = db.collection(list);
//         let ppl = col.find({}).map(data => {
//             return {
//                 Name: data.Name,
//                 Email: data.Email
//             }
//         });

//         let newFile = csvExportFolder + list + '.csv';
//         // Write to new .json file
//         var writer = fs.createWriteStream(newFile, {
//             flags: 'a', // 'a' means appending (old data will be preserved)
//             encoding: 'utf-8'
//         });

//         ppl.forEach((data) => {
//             let row = `${data.Name}, ${data.Email}\n`;
//             console.log(row);
//             writer.write(row);
//         });
//     });
// }