const net = require('net');

const readAsync = (s) => new Promise((resolve, reject) => {
  s.once('data', resolve);
  s.once('error', reject);
});

const socket = net.connect(5544, '0.0.0.0', () => {
  socket.write('CREATE_QUEUE X\n');

  let i = 0;
  setInterval(async () => {
    const data = `data_${i++}`
    console.log(`Sent ${data}`);
    socket.write(`PUBLISH X ${data}\n`);
    const response = (await readAsync(socket)).toString();

    if (response !== 'OK\n') {
      process.exit(1);
    }
  }, 500);
});
