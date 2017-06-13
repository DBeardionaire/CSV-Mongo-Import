//
module.exports = function callbackToPromise(method, ...args) {
    return new Promise(function (resolve, reject) {
        return method(...args, function (err, result) {
            return err ? reject(err) : resolve(result);
        });
    });
}