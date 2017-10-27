# Deploying Gnocchi to the cloud with Kelda

`deploy.js` can be used to deploy the code here to a virtual machine in the
cloud, using [Kelda](http://docs.kelda.io).  To deploy, first you'll need to
install Kelda (you'll also need to
[install npm](https://nodejs.org/en/download/)):

```console
$ npm install -g @kelda/install
```

Next, install the Javascript dependencies of `deploy.js`:

```console
$ npm install .
```

Finally, run the Kelda blueprint. Kelda relies on a long-running daemon to
manage the machines that you've launched:

```console
$ nohup kelda daemon > daemon.log 2>&1 &
```

This command starts the Kelda daemon in the background (it's a long-running
process) and saves the logs to `daemon.log`.

Once the daemon is running, you can run `deploy.js`:

```console
$ kelda run ./deploy.js
```

`deploy.js` will start virtual machines on Amazon EC2. Kelda will look for
your AWS credentials in `~/.aws/credentials` and in the environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`; if you don't already have these
configured, check out the [Kelda docs](http://docs.kelda.io#amazon-ec2). If
you'd like to use a different cloud provider, edit the `provider` attribute of
the `Machine`s that are declared in `deploy.js`.

`deploy.js` builds off of the
[Kelda Spark blueprint](https://github.com/kelda/spark); refer to the
documentation there for more about how Spark is configured and where to find the
various Spark UIs.

You can stop the machines by running `kelda stop`.
Note that killing the Kelda daemon won't automatically kill all of the machines
in the cluster (but if you kill the daemon before killing the machines, you can
re-start it, and then run `kelda stop`).

## Building a new version of the Gnocchi code

`deploy.js` deploys a container that was built using `Dockerfile`. This
container includes all the code needed to run Gnocchi, and will be built at
runtime by Kelda (on the Kelda master) based on the code in the
`gnocchiRefactor` branch of the
[Gnocchi repository](https://github.com/nathanielparke/gnocchi).
If you'd like to compile a different version of the code, you can edit
`Dockerfile` to pull a different version of the code and re-run `deploy.js`.
