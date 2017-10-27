const kelda = require('kelda');
const spark = require('@kelda/spark');

spark.setImage("ethanj/gnocchi")

const inf = kelda.baseInfrastructure();

const workerVMs = inf.machines.filter(machine => machine.role === 'Worker');

const s = new spark.Spark(workerVMs.length - 1);
s.exposeUIToPublic();
inf.deploy(s);
