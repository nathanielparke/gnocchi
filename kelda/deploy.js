const fs = require('fs');
const kelda = require('kelda');
const spark = require('@kelda/spark');

const numSparkWorkers = 16;

// Initialize the virtual machines to run Spark.
const machine = new kelda.Machine({ provider: 'Amazon', size: 'm4.xlarge' });

// Create one machine for each Spark worker, plus one extra machine to run
// the Spark master and job driver.
const keldaWorkers = machine.replicate(numSparkWorkers + 1);
const infra = new kelda.Infrastructure(machine, keldaWorkers);

spark.setImage(new kelda.Image('gnocchi',
  fs.readFileSync('./Dockerfile', { encoding: 'utf8' })));
const s = new spark.Spark(numSparkWorkers,
  { memoryMiB: spark.getWorkerMemoryMiB(machine) });
s.exposeUIToPublic();
s.deploy(infra);
