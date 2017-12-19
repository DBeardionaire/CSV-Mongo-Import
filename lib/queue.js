// const PQueue = require('p-queue');

// const queue = new PQueue({concurrency: 1});

// ^^ OR >
function queue(fn) {
    let lastPromise = Promise.resolve();
    return function (req) {
        let returnedPromise = lastPromise.then(() => fn(req));
        // If `returnedPromise` rejected, swallow the rejection for the queue,
        // but `returnedPromise` rejections will still be visible outside the queue
        lastPromise = returnedPromise.catch(() => { });
        return returnedPromise;
    };
}

exports = queue;