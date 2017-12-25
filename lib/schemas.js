const mongoose = require('mongoose');
require('mongoose-type-email');

const publicUserSchema = new mongoose.Schema({
    License_Number: {
        type: Number,
        required: true,
        unique : true,
        trim: true,
    },
    Email: {
        type: mongoose.SchemaTypes.Email,
        required: true,
        trim: true,
        lowercase: true,
        // unique : true,
        // allowBlank: true,
    },
    // FullName
    Name: {
        type: String,
        required: true,
        trim: true,
    },
    First_Name: { type: String, trim: true, },
    Last_Name: { type: String, trim: true, },
    Phone: { type: Number, },
    County: { type: String, trim: true, },
    License_Type: { type: String, trim: true, },
    License_Status: { type: String, trim: true, },
    Expire_Date: { type: Date },
    Origin_Date: { type: Date },
    Address: {
        Street: { type: String, trim: true, },
        City: { type: String, trim: true, },
        State: { type: String, trim: true, },
        Zip: { type: String, trim: true, },
    },
    // meta
    Unsubscribe: { type: Boolean, default: false },
});

const unsubscriberSchema = new mongoose.Schema({
    Email: {
        type: mongoose.SchemaTypes.Email,
        required: true,
        unique : true,
        trim: true,
        // lowercase: true
        // allowBlank: true,
    },
    // FullName
    Name: {
        type: String,
        trim: true,
    },
})

module.exports = { 
    publicUserSchema,
    unsubscriberSchema
}