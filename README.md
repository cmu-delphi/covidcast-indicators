# Covidcast Indicators

Pipeline code and supporting libaries for the **Real-time COVID-19 Indicators** used in the Delphi Group's **COVIDcast** map [<https://covidcast.cmu.edu>].

## The indicators

Each subdirectory contained here that is named after an indicator has specific documentation. Please review as necessary!

## General workflow for indicators creation and deployment

**tl;dr**

- Create your new indicator branch from `main`.
- Bulid it according to the established guidelines.
- Make some stuff!
- When your stuff works you can create your remote tracking branch.
- Consult with a platform engineer for the remaining production setup needs.
- Initiate a pull request against the corresponding `deploy-*` branch for your indicator.
- If your peers and Jenkins likes it then merge it to deploy.
- Rejoice!

### Starting out

The `main` branch should contain up-to-date code and supporting libraries This should be your starting point when creating a new indicator.

```shell
# Hint
#
git checkout main
git checkout -b my-feature-branch
```

### Creating your indicator

Review an existing indicator's directory to gain a sense of the pattern(s) to follow (replicate the directory structure, general code structure, linting and test guidelines, etc.)

- Consult your peers with questions! :handshake:

Once you have something that runs locally and passes tests you set up your remote branch eventual review and production deployment.

```shell
# Hint
#
git push -u origin my-feature-branch
```

### Setting up for review and deployment

Once you have your branch set up you should get in touch with a platform engineer to pair up on the remaining production needs. Tasks that may need to be taken care of are:

- Create the corresponding `deploy-*` branch in the repo.
- Add the necessary Jenkins scripts for your indicator.
- Prep the runtime host with any Automation configuration necessities.
- Generally review the workflow to makes sure it meets the general guidelines and will run as expected on the runtime host.

Once all the last mile configuration is in place you can create a pull request against the correct `deploy-*` branch to initiate the CI/CD pipeline which will build, test, and package your indicator for deployment.

If everything looks ok, platform engineering has validated the last mile, and the pull request is accepted, merge and deployment should start.

Hopefully it'll be a full on :tada: after that :crossed_fingers:

If not, circle back and try again.

## Production overview

### Running production code

Currently, the production indicators all live and run on the venerable and perennially useful Delphi primary server (also known generically as "the runtime host").

- This is a virtual machine running RHEL 7.5 and living in CMU's Campus Cloud vSphere-based infrastructure environemnt.

### Delivering an indicator to the production environment

We use a branch-based git workflow coupled with [Jenkins](https://www.jenkins.io/) and [Ansible](https://www.ansible.com/) to build, test, package, and deploy each indicator individually to the runtime host.

- Jenkins dutifully manages the whole process for us by executing several "stages" in the context of a [CI/CD pipeline](https://dzone.com/articles/learn-how-to-setup-a-cicd-pipeline-from-scratch). Each stage does something unique, building on the previous stage. The stages are:
  - Environment - Sets up some environment-specific needs that the other stages depend on.
  - Build - Create the Python venv on the Jenkins host.
  - Test - Run linting and unit tests.
  - Package - Tar and gzip the built environment.
  - Deploy - Trigger an Ansible playbook to place the built package onto the runtime host, place any necessary production configuration, and adjust the runtime envirnemnt (if necessary).

There are several additional Jenkins-specific files that will need to be created for each indicator, as well as some configuration additions to the runtime host. It will be important to pair with a platform engineer to prepare the necessary production environment needs, test the workflow, validate on production, and ultimately sign off on a production release.
