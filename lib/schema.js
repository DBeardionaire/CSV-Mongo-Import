const mongoose = require('mongoose');

const publicUserSchema = new mongoose.Schema({
    License_Number: {
        type: Number,
        required: true,
    },
    Email: {
        type: String,
        required: true,
        // lowercase: true
    },
    // FullName
    Name: {
        type: String,
        required: true,
    },
    First_Name: { type: String, },
    Last_Name: { type: String, },
    Phone: {},
    County: { type: String, },
    License_Type: { type: String, },
    License_Status: { type: String },
    Expire_Date: { type: Date },
    Origin_Date: { type: Date },
    Address: {
        Street: String,
        City: String,
        State: String,
        Zip: String,
    },
    // meta
    Unsubscribe: { type: Boolean, default: false },
});

exports = publicUserSchema