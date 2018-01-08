const mongoose = require('mongoose');
require('mongoose-type-email');
const UNSUB_ENUM = ['', 'UNSUBSCRIBE','BOUNCED', 'SPAM'];

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
    DoNotSend: { type: String, enum: UNSUB_ENUM, default: '' },
    Unsubscribe: { type: Boolean, default: false },
});

if (!publicUserSchema.options.toObject) publicUserSchema.options.toObject = {};
publicUserSchema.options.toObject.transform = function (doc, ret, options) {
    if (options.hide) {
      options.hide.split(' ').forEach(function (prop) {
        delete ret[prop];
      });
    }
    // TODO: Flatten Address
    return ret;
  }

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
    UnsubscribeType: {
        type: String,
        enum : UNSUB_ENUM,
        default: UNSUB_ENUM[1], // UNSUBSCRIBE
    } 
})

module.exports = { 
    publicUserSchema,
    unsubscriberSchema,
    UNSUB_ENUM,
}