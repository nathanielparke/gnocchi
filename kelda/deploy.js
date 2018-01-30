const kelda = require('kelda');
const spark = require('@kelda/spark');
const { allowTraffic, publicInternet } = require('kelda');

const numSparkWorkers = 2;

// Initialize the virtual machines to run Spark.
const machine = new kelda.Machine({ provider: 'Amazon', size: 't2.micro', preemptible: true, region: 'us-west-2' });

// Create one machine for each Spark worker, plus one extra machine to run
// the Spark master and job driver.
const keldaWorkers = machine.replicate(numSparkWorkers + 1);
const infra = new kelda.Infrastructure(machine, keldaWorkers);

const image = new kelda.Image('nathanielparke/gnocchi');

spark.setImage(image);
const s = new spark.Spark(numSparkWorkers,
  { memoryMiB: spark.getWorkerMemoryMiB(machine) });
//s.exposeUIToPublic();
allowTraffic(publicInternet, s.master, 22)
s.deploy(infra);
