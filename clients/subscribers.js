const net = require('net');
const stream = require('stream');

class NewLineTransform extends stream.Transform {
  constructor(opts) {
    super(opts);
    this.acc = '';
  }
  _transform(chunk, encoding, cb) {
    const s = `${this.acc}${chunk.toString()}`;
    let acc = "";
    let idx = 0;

    while (idx < s.length) {
      const char = s[idx];

      if (char === '\n') {
        this.push(acc);
        acc = "";
        idx++;
        continue;
      }

      acc += char;
      idx++;
    }

    this.acc = acc;
    cb();
  }
}

function connectSubscriber(id) {
  const socket = net.connect(5544, '0.0.0.0', () => {
    socket.write('SUBSCRIBE X\n');
    const nl = socket.pipe(new NewLineTransform());

    nl.once('data', (ch) => {
      if (ch.toString() === 'OK') {
        nl.on('data', (s) => {
          const line = s.toString();
          console.log('line =>', line);
          const [id, data] = line.split(' ');

          socket.write(`RESPONSE SUCCESS ${id}\n`);
        });
      }
    })
  });
}

connectSubscriber('FIRST');
// connectSubscriber('SECOND');