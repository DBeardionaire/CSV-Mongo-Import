//
module.exports = function sequentialCallback(...fns) {
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