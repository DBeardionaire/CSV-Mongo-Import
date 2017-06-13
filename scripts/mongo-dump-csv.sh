OIFS=$IFS;
IFS=",";

# fill in your details here
dbname=RS101
user=USERNAME
pass=PASSWORD
host=mongodb://127.0.0.1:27017 #HOSTNAME:PORT mongodb://127.0.0.1:27017

# first get all collections in the database #-u $user -p $pass
collections=`"C:\\mongodb\\bin\\mongo" "$host/$dbname" --eval "rs.slaveOk();db.getCollectionNames();"`;
collections=`"C:\\mongodb\\bin\\mongo" $dbname --eval "rs.slaveOk();db.getCollectionNames();"`;
collectionArray=($collections);

# for each collection
for ((i=0; i<${#collectionArray[@]}; ++i));
do
    echo 'exporting collection' ${collectionArray[$i]}
    # get comma separated list of keys. do this by peeking into the first document in the collection and get his set of keys
    #-u $user -p $pass 
    keys=`"C:\\mongodb\\bin\\mongo" "$host/$dbname" --eval "rs.slaveOk();var keys = []; for(var key in db.${collectionArray[$i]}.find().sort({_id: -1}).limit(1)[0]) { keys.push(key); }; keys;" --quiet`;
    "C:\\mongodb\\bin\\mongo" "mongodb://localhost:27017/RS101" --eval "rs.slaveOk();var keys = []; for(var key in db.getCollection('Expire-Mar-31-2017').find().sort({_id: -1}).limit(1)[0]) { keys.push(key); }; keys;" --quiet
    # now use mongoexport with the set of keys to export the collection to csv
    # --host $host -u $user -p $pass 
    collectionArray[$i]=${collectionArray[$i]/-/''}
    echo 'test:' ${collectionArray[$i]}
    "C:\\mongodb\\bin\\mongoexport" -d $dbname -c ${collectionArray[$i]} --fields "$keys" --csv --out C:\\temp\\$dbname.${collectionArray[$i]}.csv;
    "C:\\mongodb\\bin\\mongoexport" -d RS101 -c "Expire-Mar-31-2017" --fields "Name","Email","Phone","County" --type csv --out "C:\\FileLoader\\RS101-Expire-03312017.csv" -v

# ["_id","License_Number","Email","First_Name","Last_Name","Phone","County","License_Type","License_Status","Expire_Date","Origin_Date"]
    read test
done

IFS=$OIFS;


# "C:\\mongodb\\bin\\mongoexport" -d RS101 -c "BAY-SL-Phones-2Years" --fields "Name","Phone","Email","County","License_Type","License_Status","License_Number","Expire_Date","Origin_Date" --type csv --out "C:\\FileLoader\\Bay-SL-Phones-2-Years.csv" -v