module.exports = function run(fn) {  
  var gen = fn();

  function next(err, res) {
    if (err) return gen.throw(err);
    var ret = gen.next(res);
    if (ret.done) return;
    ret.value(next);
  }

  next();
};